package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"streamer/internal/buffer"
	"streamer/internal/config"
	"streamer/internal/metrics"
	"streamer/internal/packet"
	"streamer/internal/udp"
	"streamer/internal/utils"
)

// Глобальные переменные для параметров командной строки клиента
var (
	clientAddr    string
	clientCtrl    string
	outputFile    string
	retxTimeoutMs int
	debugMode     bool
	bufferSize    int
	numWorkers    int
	directIO      bool
)

// ClientStats - статистика работы клиента
type ClientStats struct {
	StartTime       time.Time
	EndTime         time.Time
	TotalChunks     uint64
	TotalBytes      uint64
	LostPackets     uint64
	RecoveredPackets uint64
	Duration        time.Duration
	NetworkSpeed    float64 // MB/s
	DiskWriteSpeed  float64 // MB/s
	FecEfficiency   float64 // %
}

// clientStats определена в utils.go как глобальная переменная

// printClientStats выводит статистику работы клиента
func printClientStats(out io.Writer, outputFile string) {
	utils.GlobalClientStats.EndTime = time.Now()
	utils.GlobalClientStats.Duration = utils.GlobalClientStats.EndTime.Sub(utils.GlobalClientStats.StartTime)

	if utils.GlobalClientStats.Duration.Seconds() > 0 {
		utils.GlobalClientStats.NetworkSpeed = float64(utils.GlobalClientStats.TotalBytes) / utils.GlobalClientStats.Duration.Seconds() / (1024 * 1024) // MB/s
	}

	// Расчет эффективности FEC
	totalLost := utils.GlobalClientStats.LostPackets + utils.GlobalClientStats.RecoveredPackets
	if totalLost > 0 {
		utils.GlobalClientStats.FecEfficiency = float64(utils.GlobalClientStats.RecoveredPackets) / float64(totalLost) * 100
	}

	// Расчет скорости записи на диск
	if outputFile != "-" {
		if bf, ok := out.(*buffer.BufferedFileWriter); ok {
			utils.GlobalClientStats.DiskWriteSpeed = bf.GetWriteSpeed()
		}
	}

	fmt.Printf("\n[CLIENT] === Reception Statistics ===\n")
	fmt.Printf("[CLIENT] Duration: %.2fs\n", utils.GlobalClientStats.Duration.Seconds())
	fmt.Printf("[CLIENT] Total chunks: %d\n", utils.GlobalClientStats.TotalChunks)
	fmt.Printf("[CLIENT] Total bytes: %d (%.2f MB)\n", utils.GlobalClientStats.TotalBytes, float64(utils.GlobalClientStats.TotalBytes)/(1024*1024))
	fmt.Printf("[CLIENT] Network speed: %.2f MB/s\n", utils.GlobalClientStats.NetworkSpeed)
	fmt.Printf("[CLIENT] Disk write speed: %.2f MB/s\n", utils.GlobalClientStats.DiskWriteSpeed)
	fmt.Printf("[CLIENT] Lost packets: %d\n", utils.GlobalClientStats.LostPackets)
	fmt.Printf("[CLIENT] Recovered packets: %d\n", utils.GlobalClientStats.RecoveredPackets)
	fmt.Printf("[CLIENT] FEC efficiency: %.1f%%\n", utils.GlobalClientStats.FecEfficiency)
	fmt.Printf("[CLIENT] ============================\n")
}

var ClientCmd = &cobra.Command{
	Use:   "client",
	Short: "Run in client mode",
	Run: func(cmd *cobra.Command, args []string) {
		config.DebugEnabled = debugMode
		log.SetOutput(os.Stderr)
		// Инициализация статистики
		utils.GlobalClientStats.StartTime = time.Now()
		utils.GlobalClientStats.TotalChunks = 0
		utils.GlobalClientStats.TotalBytes = 0
		utils.GlobalClientStats.LostPackets = 0
		utils.GlobalClientStats.RecoveredPackets = 0

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := utils.SetupGracefulShutdown()

		host, ports, err := utils.ParseAddressWithPorts(clientAddr)
		if err != nil {
			log.Fatalf("failed to parse client address: %v", err)
		}
	
		// Keep original host for localhost UDP
		if host == "127.0.0.1" || host == "localhost" {
			log.Printf("[CLIENT] Using localhost (%s) for UDP communication", host)
		}

		fmt.Printf("[CLIENT] starting... host=%s ports=%v\n", host, ports)
		metrics.PromActivePorts.Set(float64(len(ports)))

		receiver, err := udp.NewMultiUDPReceiver(host, ports, 10000000)
		if err != nil {
			log.Fatalf("failed to create multi UDP receiver: %v", err)
		}
		defer receiver.Close()

		log.Printf("[CLIENT] UDP receiver created successfully for %v", ports)
		log.Printf("[CLIENT] Packet channel capacity: %d", cap(receiver.Packets()))

		// RETX connection variables - will be initialized after first UDP packet
		var ctrlConn net.Conn
		var ctrlEnc *json.Encoder
		var retxEnabled bool
		var firstPacketReceived bool

		// Declare function variable (will be assigned after rb is created)
		var attemptRetxConnection func()

		// Control connection cleanup will be handled when connection is established

		var out io.Writer
		if outputFile == "-" {
			out = os.Stdout
		} else {
			// Use buffered writer for better disk I/O performance
			if bufferSize <= 0 {
				bufferSize = 128 * 1024 * 1024 // 128MB default
			}
			if numWorkers <= 0 {
				numWorkers = 4 // Default to 4 workers
			}
			f, err := buffer.NewBufferedFileWriterWithWorkersAndDirectIO(outputFile, bufferSize, numWorkers, directIO)
			if err != nil {
				log.Fatalf("create output: %v", err)
			}
			defer f.Close()
			out = f
		}

		rb := buffer.NewReorderBuffer(out, time.Duration(retxTimeoutMs)*time.Millisecond, func(chunkID uint64, missing []uint32) {
			log.Printf("[CLIENT] RETX: Callback triggered for chunk %d, missing packets: %v", chunkID, missing)
			// Establish RETX connection on first use (after receiving first UDP packet)
			if !firstPacketReceived {
				log.Printf("[CLIENT] ERROR: RETX requested before first UDP packet received! Chunk: %d, missing: %v", chunkID, missing)
				return
			}

			if !retxEnabled || ctrlConn == nil || ctrlEnc == nil {
				log.Printf("[CLIENT] RETX: Connection not ready (enabled=%v, conn=%v, enc=%v), attempting to connect...", retxEnabled, ctrlConn != nil, ctrlEnc != nil)
				attemptRetxConnection()
				// Give it more time to connect
				time.Sleep(100 * time.Millisecond)
				log.Printf("[CLIENT] RETX: After connection attempt - enabled=%v, conn=%v, enc=%v", retxEnabled, ctrlConn != nil, ctrlEnc != nil)
			}
			if ctrlEnc == nil {
				log.Printf("[CLIENT] ERROR: RETX not available - control connection failed for chunk %d", chunkID)
				return
			}
			if err := ctrlEnc.Encode(map[string]any{"type": "retx", "chunk": chunkID, "missing": missing}); err != nil {
				log.Printf("[CLIENT] ERROR: Failed to send RETX request for chunk %d: %v", chunkID, err)
				// Reset connection on error
				if ctrlConn != nil {
					ctrlConn.Close()
					ctrlConn = nil
					ctrlEnc = nil
					retxEnabled = false
				}
			} else {
				log.Printf("[CLIENT] SUCCESS: RETX request sent for chunk %d (%d missing packets: %v)", chunkID, len(missing), missing)
			}
		})

		// Function to attempt RETX connection (defined after rb is created)
		attemptRetxConnection = func() {
			if retxEnabled {
				log.Printf("[CLIENT] RETX: Already connected, skipping connection attempt")
				return // Already connected
			}
			log.Printf("[CLIENT] RETX: Attempting TCP connection to %s", clientCtrl)
			conn, err := net.Dial("tcp", clientCtrl)
			if err != nil {
				log.Printf("[CLIENT] ERROR: RETX TCP connection failed: %v", err)
				return
			}
			ctrlConn = conn
			ctrlEnc = json.NewEncoder(ctrlConn)
			retxEnabled = true
			log.Printf("[CLIENT] SUCCESS: RETX TCP connection established to %s", clientCtrl)

			// Start control client handler now that we have a connection
			log.Printf("[CLIENT] RETX: Starting control client handler goroutine")
			go handleControlClient(ctrlConn, rb)
		}

		// Control client handler will be started when RETX connection is established

		go func() {
			log.Printf("[CLIENT] Starting receiver error handler")
			for err := range receiver.Errors() {
				if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
					log.Printf("[CLIENT] Receiver error: %v", err)
				}
			}
			log.Printf("[CLIENT] Receiver error handler exited")
		}()

		go func() {
			<-sigChan
			log.Println("[CLIENT] Received shutdown signal")
			cancel()
		}()

		log.Println("[CLIENT] Starting packet reception...")

		// Start with a shorter initial timeout to detect if server is running
		inactivityTimeout := time.NewTimer(10 * time.Second)
		defer inactivityTimeout.Stop()
		initialTimeout := true

		for {
			select {
			case packetData, ok := <-receiver.Packets():
				if !ok {
					log.Println("[CLIENT] Packet channel closed")
					goto cleanup
				}

				utils.DebugLog("[CLIENT] Received raw packet data, size: %d bytes", len(packetData))
				utils.DebugLog("[CLIENT] Processing packet in main loop...")
				if len(packetData) < 36 {
					log.Printf("[CLIENT] ERROR: Packet too small (%d bytes), minimum header is 36 bytes - DROPPING PACKET", len(packetData))
					atomic.AddUint64(&utils.GlobalClientStats.LostPackets, 1)
					continue
				}

				if !inactivityTimeout.Stop() {
					<-inactivityTimeout.C
				}
				inactivityTimeout.Reset(60 * time.Second)

				pkt, err := packet.UnmarshalPacket(packetData)
				if err != nil {
					log.Printf("[CLIENT] ERROR: Bad packet unmarshal error: %v - DROPPING PACKET", err)
					atomic.AddUint64(&utils.GlobalClientStats.LostPackets, 1)
					continue
				}

				utils.DebugLog("[CLIENT] SUCCESS: Unmarshaled packet: chunk=%d, packet=%d, totalPkts=%d, fecType=%d, payload=%d bytes",
					pkt.ChunkID, pkt.PacketID, pkt.TotalPkts, pkt.FECType, len(pkt.Payload))

				// Skip END markers for statistics
				if pkt.PacketID == config.EndOfStreamMarker {
					log.Printf("[CLIENT] INFO: Received END marker for chunk %d", pkt.ChunkID)
					// Still process END markers for stream completion
					rb.Add(pkt)
					continue
				}

				// Обновление статистики приема
				atomic.AddUint64(&utils.GlobalClientStats.TotalBytes, uint64(len(pkt.Payload)))

				utils.DebugLog("[CLIENT] Adding packet to reorder buffer: chunk=%d, packet=%d", pkt.ChunkID, pkt.PacketID)
				rb.Add(pkt)
				utils.DebugLog("[CLIENT] Packet added to reorder buffer successfully")

				// Mark that we've received the first packet - now safe to attempt RETX connection
				if !firstPacketReceived {
					firstPacketReceived = true
					log.Printf("[CLIENT] INFO: First UDP packet received - RETX connection now allowed")
				}

				utils.DebugLog("[CLIENT] Checking completion status...")
				if rb.IsComplete() {
					log.Printf("[CLIENT] INFO: Stream completed. Shutting down...")
					goto cleanup
				}

				utils.DebugLog("[CLIENT] Loop iteration completed, waiting for next packet...")
				utils.DebugLog("[CLIENT] Receiver still active: %v", receiver != nil)
				utils.DebugLog("[CLIENT] About to enter select...")

			case <-rb.GetCompletionNotify():
				log.Println("[CLIENT] Stream completion notification received. Shutting down...")
				goto cleanup

			case <-time.After(5 * time.Second):
				log.Printf("[CLIENT] Still waiting for packets... (inactivity timeout in %d seconds)", 55)

			case <-inactivityTimeout.C:
				timeoutDuration := 60
				if initialTimeout {
					timeoutDuration = 10
					initialTimeout = false
				}
				log.Printf("[CLIENT] INACTIVITY TIMEOUT: No packets received for %d seconds. This indicates the UDP receiver stopped working!", timeoutDuration)
				goto cleanup

			case <-ctx.Done():
				log.Println("[CLIENT] Context cancelled, shutting down...")
				goto cleanup
			}
		}

	cleanup:
		log.Println("[CLIENT] Starting cleanup...")
		cancel()

		// Flush any remaining buffered data
		if outputFile != "-" {
			if bf, ok := out.(*buffer.BufferedFileWriter); ok {
				if err := bf.Flush(); err != nil {
					log.Printf("[CLIENT] Error flushing buffered writer: %v", err)
				}
			}
		}

		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			receiver.Close()
		}()

		select {
		case <-closeDone:
			log.Println("[CLIENT] Receiver closed successfully")
		case <-time.After(5 * time.Second):
			log.Println("[CLIENT] Receiver close timeout, forcing exit")
		}

		// Вывод статистики перед завершением
		printClientStats(out, outputFile)

		log.Println("[CLIENT] Client shutdown complete")
	},
}

func init() {
	ClientCmd.Flags().StringVar(&clientAddr, "addr", "127.0.0.1:5001", "UDP source (supports multiple ports: host:port1,port2,port3-port5) - use different port than server")
	ClientCmd.Flags().StringVar(&clientCtrl, "ctrl", "127.0.0.1:6000", "TCP control to server")
	ClientCmd.Flags().StringVar(&outputFile, "output", "-", "output file ('-' = stdout)")
	ClientCmd.Flags().IntVar(&retxTimeoutMs, "retx-timeout", 1000, "ms to wait before RETX request for oldest incomplete chunk (default: 1000ms)")
	ClientCmd.Flags().BoolVar(&debugMode, "debug", false, "enable debug logging")
	ClientCmd.Flags().IntVar(&bufferSize, "buffer-size", 128*1024*1024, "disk write buffer size in bytes")
	ClientCmd.Flags().IntVar(&numWorkers, "workers", 4, "number of async write workers")
	ClientCmd.Flags().BoolVar(&directIO, "direct-io", false, "use Direct I/O (Linux only, bypasses OS cache)")
}

// handleControlClient handles incoming RETX packets from server
func handleControlClient(conn net.Conn, rb *buffer.ReorderBuffer) {
	log.Printf("[CLIENT] RETX: Control client handler started")

	buf := make([]byte, 64*1024) // Large buffer for packets

	for {
		log.Printf("[CLIENT] RETX: Waiting for RETX packets from server...")
		n, err := conn.Read(buf)
		if err != nil {
			if !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "EOF") {
				log.Printf("[CLIENT] ERROR: Control TCP read error: %v", err)
			} else {
				log.Printf("[CLIENT] INFO: Control TCP connection closed: %v", err)
			}
			return
		}

		if n == 0 {
			log.Printf("[CLIENT] RETX: Received 0 bytes, continuing...")
			continue
		}

		log.Printf("[CLIENT] RETX: Received %d bytes from server", n)

		// Parse the RETX packet
		pkt, err := packet.UnmarshalPacket(buf[:n])
		if err != nil {
			log.Printf("[CLIENT] ERROR: Failed to unmarshal RETX packet: %v", err)
			continue
		}

		log.Printf("[CLIENT] SUCCESS: Received RETX packet: chunk %d, packet %d (%d bytes payload)", pkt.ChunkID, pkt.PacketID, len(pkt.Payload))

		// Feed the RETX packet to the reorder buffer
		log.Printf("[CLIENT] RETX: Feeding packet to reorder buffer...")
		rb.FeedRetxPacket(pkt)

		log.Printf("[CLIENT] SUCCESS: Processed RETX packet for chunk %d", pkt.ChunkID)
	}
}