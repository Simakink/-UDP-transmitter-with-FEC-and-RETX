package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	rs "github.com/klauspost/reedsolomon"
	"streamer/internal/buffer"
	"streamer/internal/config"
	"streamer/internal/metrics"
	"streamer/internal/packet"
	"streamer/internal/udp"
	"streamer/internal/utils"
)

// Глобальные переменные для параметров командной строки сервера
var (
	serverAddr      string
	serverCtrl      string
	chunkBytes      int
	fecTypeFlag     string
	fecK            int
	fecR            int
	ringSeconds     int
	inputFile       string
	backend         string
	sendWorkers     int
	poolSize        int
	chunkIntervalMs int
	packetPayload   int
	prometheusAddr  string
	debugMode       bool
	gracePeriodSec  int
)

// ServerStats - статистика работы сервера
type ServerStats struct {
	StartTime     time.Time
	EndTime       time.Time
	TotalChunks   uint64
	TotalBytes    uint64
	RetxRequests  uint64
	Duration      time.Duration
	NetworkSpeed  float64 // MB/s
}

var serverStats ServerStats

// printServerStats выводит статистику работы сервера
func printServerStats() {
	serverStats.EndTime = time.Now()
	serverStats.Duration = serverStats.EndTime.Sub(serverStats.StartTime)

	if serverStats.Duration.Seconds() > 0 {
		serverStats.NetworkSpeed = float64(serverStats.TotalBytes) / serverStats.Duration.Seconds() / (1024 * 1024) // MB/s
	}

	fmt.Printf("\n[SERVER] === Transmission Statistics ===\n")
	fmt.Printf("[SERVER] Duration: %.2fs\n", serverStats.Duration.Seconds())
	fmt.Printf("[SERVER] Total chunks: %d\n", serverStats.TotalChunks)
	fmt.Printf("[SERVER] Total bytes: %d (%.2f MB)\n", serverStats.TotalBytes, float64(serverStats.TotalBytes)/(1024*1024))
	fmt.Printf("[SERVER] Network speed: %.2f MB/s\n", serverStats.NetworkSpeed)
	fmt.Printf("[SERVER] RETX requests: %d\n", serverStats.RetxRequests)
	fmt.Printf("[SERVER] FEC type: %s\n", fecTypeFlag)
	if fecTypeFlag == "rs" {
		fmt.Printf("[SERVER] RS parameters: k=%d, r=%d\n", fecK, fecR)
	}
	fmt.Printf("[SERVER] =================================\n")
}

var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "Run in server mode",
	Run: func(cmd *cobra.Command, args []string) {
		config.DebugEnabled = debugMode

		log.SetOutput(os.Stderr)

		sigChan := utils.SetupGracefulShutdown()

		if err := validateServerParams(); err != nil {
			log.Fatalf("parameter validation failed: %v", err)
		}

		host, ports, err := utils.ParseAddressWithPorts(serverAddr)
		if err != nil {
			log.Fatalf("failed to parse server address: %v", err)
		}

		// Инициализация статистики
		serverStats.StartTime = time.Now()
		serverStats.TotalChunks = 0
		serverStats.TotalBytes = 0
		serverStats.RetxRequests = 0

		fmt.Printf("[SERVER] starting... host=%s ports=%v ctrl=%s chunkBytes=%d fec=%s ring=%ds workers=%d\n",
			host, ports, serverCtrl, chunkBytes, fecTypeFlag, ringSeconds, sendWorkers)
		if debugMode {
			fmt.Printf("[SERVER] debug: input=%s backend=%s pool=%d interval=%dms payload=%d prometheus=%s grace-period=%ds\n",
				inputFile, backend, poolSize, chunkIntervalMs, packetPayload, prometheusAddr, gracePeriodSec)
		}

		metrics.PromActivePorts.Set(float64(len(ports)))

		slots := utils.Max(1000, ringSeconds*(1000/utils.Max(1, chunkIntervalMs))*2) // Double the size for RETX
		ringBuf := buffer.NewRingBuffer(slots)
		fmt.Printf("[SERVER] ring buffer initialized with %d slots for %d ports\n", slots, len(ports))

		metrics.StartPrometheus(prometheusAddr)

		// Initialize RS encoder if needed
		var rsEnc interface{}
		if fecTypeFlag == "rs" {
			enc, err := rs.New(fecK, fecR)
			if err != nil {
				log.Fatalf("reedsolomon init: %v", err)
			}
			rsEnc = enc
		}

		go startControlServer(serverCtrl, ringBuf)

		go func() {
			<-sigChan
			log.Println("[SERVER] Received shutdown signal. Graceful shutdown initiated...")
			os.Exit(0)
		}()

		startFileSenderMulti(host, ports, ringBuf, fecTypeFlag, rsEnc)
	},
}

func init() {
	ServerCmd.Flags().StringVar(&serverAddr, "addr", "239.0.0.1:5000", "UDP destination (supports multiple ports: host:port1,port2,port3-port5)")
	ServerCmd.Flags().StringVar(&serverCtrl, "ctrl", ":6000", "TCP control listen address")
	ServerCmd.Flags().IntVar(&chunkBytes, "chunk-bytes", 1<<20, "bytes per chunk (before split to packets)")
	ServerCmd.Flags().StringVar(&fecTypeFlag, "fec", "none", "fec type (none|xor|rs)")
	ServerCmd.Flags().IntVar(&fecK, "fec-k", 3, "reed-solomon data shards")
	ServerCmd.Flags().IntVar(&fecR, "fec-r", 1, "reed-solomon parity shards")
	ServerCmd.Flags().IntVar(&ringSeconds, "ring", 60, "ring buffer seconds")
	ServerCmd.Flags().StringVar(&inputFile, "input", "-", "input file, '-' = stdin")
	ServerCmd.Flags().StringVar(&backend, "backend", "std", "send backend: std|io_uring")
	ServerCmd.Flags().IntVar(&sendWorkers, "workers", 2, "send workers for std backend")
	ServerCmd.Flags().IntVar(&poolSize, "pool", 8192, "initial packet buffer pool size")
	ServerCmd.Flags().IntVar(&chunkIntervalMs, "interval", 200, "ms between chunks (for ring sizing)")
	ServerCmd.Flags().IntVar(&packetPayload, "packet-payload", 1200, "UDP payload per packet (bytes)")
	ServerCmd.Flags().StringVar(&prometheusAddr, "metrics-addr", ":9100", "prometheus listen address")
	ServerCmd.Flags().BoolVar(&debugMode, "debug", false, "enable debug logging")
	ServerCmd.Flags().IntVar(&gracePeriodSec, "grace-period", 30, "grace period for server shutdown (seconds)")
}

func validateServerParams() error {
	if chunkBytes <= 0 || chunkBytes > 100<<20 {
		return fmt.Errorf("invalid chunk-bytes: %d", chunkBytes)
	}
	if packetPayload <= 0 || packetPayload > 64*1024 {
		return fmt.Errorf("invalid packet-payload: %d", packetPayload)
	}
	if fecTypeFlag == "rs" {
		if fecK <= 0 || fecR < 0 || fecK+fecR > 255 {
			return fmt.Errorf("invalid RS parameters: k=%d r=%d", fecK, fecR)
		}
		// Check if RS shard size fits in packet payload
		shardSize := (chunkBytes + fecK - 1) / fecK
		if shardSize > packetPayload {
			return fmt.Errorf("RS shard size %d exceeds packet payload %d, increase --packet-payload or decrease --fec-k", shardSize, packetPayload)
		}
	}
	if gracePeriodSec < 0 || gracePeriodSec > 3600 {
		return fmt.Errorf("invalid grace-period: %d (must be 0-3600 seconds)", gracePeriodSec)
	}
	return nil
}

// startControlServer запускает TCP сервер для обработки RETX запросов
func startControlServer(addr string, rb *buffer.RingBuffer) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("control listen: %v", err)
	}
	defer ln.Close()

	log.Printf("[CTRL] listening on %s", addr)

	// Принимаем соединения в бесконечном цикле
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		// Enable TCP_NODELAY for low-latency RETX requests
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
		}
		// Каждое соединение обрабатываем в отдельной goroutine
		go handleControl(conn, rb)
	}
}

func startFileSenderMulti(host string, ports []int, rb *buffer.RingBuffer, fecType string, rsEnc interface{}) {
	// Создаем множественные UDP соединения
	sender, err := udp.NewMultiUDPSender(host, ports)
	if err != nil {
		log.Fatalf("failed to create multi UDP sender: %v", err)
	}
	defer sender.Close()

	// Подготовка входного потока данных
	var in io.Reader
	if inputFile == "-" {
		in = os.Stdin
	} else {
		f, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("open input: %v", err)
		}
		defer f.Close()
		in = bufio.NewReader(f) // Буферизация для улучшения производительности
	}

	// Предзаполнение packet pool для уменьшения GC давления
	for i := 0; i < poolSize; i++ {
		buffer.GetPBuf() // This will create buffers in the pool
	}

	// Создание канала и worker'ов для отправки пакетов
	sendQSize := 16384 * len(ports)
	sendQ := make(chan *buffer.PBuf, sendQSize)
	var wg sync.WaitGroup

	// Счетчик UDP ошибок для раннего выхода
	var udpErrorCounter int32

	if backend == "io_uring" {
		// Высокопроизводительный io_uring backend (Linux)
		for i, conn := range sender.GetConnections() {
			wg.Add(1)
			go func(workerID int, conn *net.UDPConn) {
				defer wg.Done()
				ioUringSenderBatched(sendQ, conn, 64, &udpErrorCounter)
			}(i, conn)
		}
	} else {
		// Стандартный backend с worker'ами распределенными по портам
		workersPerPort := utils.Max(1, sendWorkers/len(ports))
		if workersPerPort == 0 {
			workersPerPort = 1
		}

		for portIdx, conn := range sender.GetConnections() {
			for workerIdx := 0; workerIdx < workersPerPort; workerIdx++ {
				wg.Add(1)
				workerID := portIdx*workersPerPort + workerIdx
				go func(id int, conn *net.UDPConn) {
					defer wg.Done()
					sendWorkerStd(id, sendQ, conn, &udpErrorCounter)
				}(workerID, conn)
			}
		}
	}

	log.Printf("[SENDER] started with %d UDP ports, %d workers total", len(ports), sendWorkers)

	// Основной цикл чтения файла и отправки chunks
	chunkBuf := make([]byte, chunkBytes)
	var chunkID uint64 = 1 // Chunks нумеруются начиная с 1
	var totalChunks uint64 = 0

	// Pre-allocate data buffer to avoid repeated allocations
	data := make([]byte, chunkBytes)

	for {
		// Читаем следующий chunk из файла
		n, err := io.ReadFull(in, chunkBuf)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			if n == 0 {
				break // Конец файла
			}
		} else if err != nil {
			log.Printf("read error: %v", err)
			break
		}

		// Reuse pre-allocated buffer instead of creating new one
		if len(data) < n {
			data = make([]byte, n)
		}
		copy(data[:n], chunkBuf[:n])

		// Определяем количество data-пакетов на основе размера полезной нагрузки
		k := int((n + packetPayload - 1) / packetPayload) // Ceiling division

		// Отправляем chunk с выбранным типом FEC через множественные порты
		switch fecType {
		case "none":
			sendChunkNoneMulti(chunkID, data, k, n, rb, sendQ, sender)
		case "xor":
			sendChunkXORMulti(chunkID, data, k, n, rb, sendQ, sender)
		case "rs":
			sendChunkRSMulti(chunkID, data, fecK, fecR, n, rb, sendQ, rsEnc, sender)
		default:
			log.Fatalf("unknown fec type: %s", fecType)
		}

		utils.DebugLog("[TX] chunk %d sent (%d bytes) via %d ports", chunkID, n, len(ports))

		// Обновление статистики
		atomic.AddUint64(&serverStats.TotalChunks, 1)
		atomic.AddUint64(&serverStats.TotalBytes, uint64(n))

		totalChunks = chunkID
		chunkID++

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}

		// Removed sleep for maximum throughput - use flow control if needed
		// time.Sleep(time.Duration(chunkIntervalMs) * time.Millisecond)
	}

	// Правильный graceful shutdown
	log.Printf("[SENDER] File transmission complete. %d chunks sent. Waiting for RETX requests...", totalChunks)

	// Отправляем END пакет для уведомления клиентов
	sendEndMarker(totalChunks, rb, sendQ, sender)

	// Используем gracePeriodSec напрямую
	gracePeriodDuration := time.Duration(gracePeriodSec) * time.Second
	log.Printf("[SENDER] Grace period: %v. Processing RETX requests...", gracePeriodDuration)

	shutdownStart := time.Now()
	gracefulShutdown := time.NewTimer(gracePeriodDuration)
	retxActivity := time.NewTicker(5 * time.Second)

	defer retxActivity.Stop()
	defer gracefulShutdown.Stop()

	for {
		select {
		case <-gracefulShutdown.C:
			log.Println("[SENDER] Grace period expired. Shutting down.")
			goto shutdown
		case <-retxActivity.C:
			elapsed := time.Since(shutdownStart)
			remaining := gracePeriodDuration - elapsed

			if remaining <= 0 {
				log.Println("[SENDER] Grace period expired (calculated). Shutting down.")
				goto shutdown
			}

			// Проверяем количество UDP ошибок
			udpErrors := atomic.LoadInt32(&udpErrorCounter)
			if udpErrors > 100 { // Если много UDP ошибок - клиенты отключились
				log.Printf("[SENDER] Many UDP errors (%d) detected. Clients likely disconnected. Shutting down early.", udpErrors)
				goto shutdown
			}

			log.Printf("[SENDER] Processing RETX requests. Remaining time: %v, UDP errors: %d", remaining, udpErrors)
		}
	}

shutdown:
	// Вывод статистики перед завершением
	printServerStats()

	// Завершение работы: ждем отправки всех пакетов
	close(sendQ)
	wg.Wait()
	log.Println("[SENDER] Graceful shutdown complete")
}

// RETXRequest represents a retransmission request from client
type RETXRequest struct {
	Type    string   `json:"type"`
	Chunk   uint64   `json:"chunk"`
	Missing []uint32 `json:"missing"`
}

// handleControl обрабатывает одно TCP соединение с RETX запросами
func handleControl(c net.Conn, rb *buffer.RingBuffer) {
	defer c.Close()

	utils.DebugLog("[CTRL] new connection from %s", c.RemoteAddr())

	// Create a decoder for reading JSON requests
	dec := json.NewDecoder(c)

	for {
		var req RETXRequest
		if err := dec.Decode(&req); err != nil {
			if !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "EOF") {
				log.Printf("[CTRL] decode error: %v", err)
			}
			return
		}

		utils.DebugLog("[CTRL] received request: type=%s, chunk=%d, missing=%d packets", req.Type, req.Chunk, len(req.Missing))

		if req.Type == "retx" {
			atomic.AddUint64(&serverStats.RetxRequests, 1)
			handleRETXRequest(c, rb, req.Chunk, req.Missing)
		}
	}
}

// handleRETXRequest processes a retransmission request
func handleRETXRequest(c net.Conn, rb *buffer.RingBuffer, chunkID uint64, missing []uint32) {
	utils.DebugLog("[RETX] processing request for chunk %d, missing packets: %v", chunkID, missing)

	// Look up the chunk in the ring buffer
	chunk, ok := rb.Get(chunkID)
	if !ok {
		log.Printf("[RETX] chunk %d not found in ring buffer", chunkID)
		return
	}

	// Send each missing packet
	for _, packetID := range missing {
		if int(packetID) >= len(chunk.Packets) {
			log.Printf("[RETX] invalid packet ID %d for chunk %d", packetID, chunkID)
			continue
		}

		pbuf := chunk.Packets[packetID]
		if pbuf == nil {
			log.Printf("[RETX] packet %d not available in chunk %d", packetID, chunkID)
			continue
		}

		// Send the packet data back to client
		data := pbuf.Bytes()
		if _, err := c.Write(data); err != nil {
			log.Printf("[RETX] failed to send packet %d for chunk %d: %v", packetID, chunkID, err)
			return
		}

		utils.DebugLog("[RETX] sent packet %d for chunk %d", packetID, chunkID)
		atomic.AddUint64(&serverStats.RetxRequests, 1)
	}

	utils.DebugLog("[RETX] completed request for chunk %d", chunkID)
}

// sendWorkerStd sends packets using standard UDP
func sendWorkerStd(id int, q <-chan *buffer.PBuf, conn *net.UDPConn, errorCounter *int32) {
	utils.DebugLog("sendWorkerStd %d started", id)
	var packetCount int

	for pb := range q {
		b := pb.Bytes()
		if len(b) == 0 {
			utils.DebugLog("worker %d: empty packet!", id)
			pb.Dec()
			continue
		}

		// Отправляем пакет по UDP
		if _, err := conn.Write(b); err != nil {
			if errorCounter != nil {
				atomic.AddInt32(errorCounter, 1)
			}

			errorCount := atomic.LoadInt32(errorCounter)
			if errorCount <= 10 {
				log.Printf("worker %d write err: %v", id, err)
			} else if errorCount == 11 {
				log.Printf("worker %d: suppressing further UDP errors (total: %d)", id, errorCount)
			}
		} else {
			packetCount++
		}

		pb.Dec()
	}

	utils.DebugLog("sendWorkerStd %d stopped (sent %d packets)", id, packetCount)
}

// ioUringSenderBatched sends packets using io_uring (placeholder for now)
func ioUringSenderBatched(q <-chan *buffer.PBuf, conn *net.UDPConn, batch int, errorCounter *int32) {
	utils.DebugLog("ioUringSender(batched) starting - falling back to std")

	// For now, just use standard sending
	sendWorkerStd(0, q, conn, errorCounter)
}

// sendEndMarker sends end-of-stream marker
func sendEndMarker(totalChunks uint64, rb *buffer.RingBuffer, sendQ chan<- *buffer.PBuf, sender *udp.MultiUDPSender) {
	// Создаем специальный END пакет
	endPkt := &packet.Packet{
		ChunkID:    totalChunks + 1,
		PacketID:   config.EndOfStreamMarker,
		TotalPkts:  1,
		ChunkBytes: 0,
		Version:    config.HeaderVersion,
		FECType:    config.FECNone,
		K:          0,
		R:          0,
		ShardSize:  0,
		DataPkts:   0,
		Payload:    []byte("END_OF_STREAM"),
	}

	// Отправляем END пакет на все порты несколько раз для надежности
	for repeat := 0; repeat < 10; repeat++ {
		for portIdx := 0; portIdx < len(sender.GetConnections()); portIdx++ {
			b := endPkt.Marshal()
			pbuf := buffer.GetPBuf()
			pbuf.Inc()

			pbuf.SetData(b)

			select {
			case sendQ <- pbuf:
			default:
				pbuf.Dec()
				log.Printf("WARNING: Cannot send END marker, queue full")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[SENDER] END markers sent (total chunks: %d)", totalChunks)
}

// sendChunkNoneMulti sends chunk without FEC
func sendChunkNoneMulti(chunkID uint64, data []byte, k, n int, rb *buffer.RingBuffer, sendQ chan<- *buffer.PBuf, sender *udp.MultiUDPSender) {
	total := k
	ch := &buffer.Chunk{ID: chunkID, Timestamp: time.Now(), PktCnt: total, Packets: make([]*buffer.PBuf, total)}
	per := (n + k - 1) / k // Ceiling division for packet size

	for i := 0; i < k; i++ {
		start := i * per
		end := start + per
		if end > n {
			end = n
		}

		shard := make([]byte, end-start)
		copy(shard, data[start:end])

		pkt := &packet.Packet{
			ChunkID:    chunkID,
			PacketID:   uint32(i),
			TotalPkts:  uint32(total),
			ChunkBytes: uint32(n),
			Version:    config.HeaderVersion,
			FECType:    config.FECNone,
			K:          uint16(k),
			R:          0,
			ShardSize:  uint32(len(shard)),
			DataPkts:   uint32(k),
			Payload:    shard,
		}
		enqueuePacketMulti(pkt, ch, sendQ)
	}
	rb.Put(ch)
}

// sendChunkXORMulti sends chunk with XOR FEC
func sendChunkXORMulti(chunkID uint64, data []byte, k, n int, rb *buffer.RingBuffer, sendQ chan<- *buffer.PBuf, sender *udp.MultiUDPSender) {
	total := k + 1
	ch := &buffer.Chunk{ID: chunkID, Timestamp: time.Now(), PktCnt: total, Packets: make([]*buffer.PBuf, total)}
	per := packetPayload

	parity := make([]byte, per)

	// Parallel XOR parity calculation
	var wg sync.WaitGroup
	parityMutex := &sync.Mutex{}

	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			start := idx * per
			end := start + per
			if end > n {
				end = n
			}

			shard := make([]byte, per)
			copy(shard, data[start:end])

			// Thread-safe parity calculation
			parityMutex.Lock()
			for j := 0; j < len(shard); j++ {
				if start+j < n {
					parity[j] ^= shard[j]
				}
			}
			parityMutex.Unlock()

			pkt := &packet.Packet{
				ChunkID:    chunkID,
				PacketID:   uint32(idx),
				TotalPkts:  uint32(total),
				ChunkBytes: uint32(n),
				Version:    config.HeaderVersion,
				FECType:    config.FECXOR,
				K:          uint16(k),
				R:          1,
				ShardSize:  uint32(per),
				DataPkts:   uint32(k),
				Payload:    shard,
			}
			enqueuePacketMulti(pkt, ch, sendQ)
		}(i)
	}

	// Wait for all data packets to be processed
	wg.Wait()

	parityPkt := &packet.Packet{
		ChunkID:    chunkID,
		PacketID:   uint32(k),
		TotalPkts:  uint32(total),
		ChunkBytes: uint32(n),
		Version:    config.HeaderVersion,
		FECType:    config.FECXOR,
		K:          uint16(k),
		R:          1,
		ShardSize:  uint32(per),
		DataPkts:   uint32(k),
		Payload:    parity,
	}
	enqueuePacketMulti(parityPkt, ch, sendQ)
	rb.Put(ch)
}

// sendChunkRSMulti sends chunk with Reed-Solomon FEC
func sendChunkRSMulti(chunkID uint64, data []byte, k, r, n int, rb *buffer.RingBuffer, sendQ chan<- *buffer.PBuf, rsEnc interface{}, sender *udp.MultiUDPSender) {
	total := k + r
	ch := &buffer.Chunk{ID: chunkID, Timestamp: time.Now(), PktCnt: total, Packets: make([]*buffer.PBuf, total)}

	// Pad data to make it divisible by k for RS
	originalN := n
	paddedSize := ((n + k - 1) / k) * k
	if paddedSize > n {
		// Pad with zeros
		padding := make([]byte, paddedSize-n)
		data = append(data[:n], padding...)
		n = paddedSize
	}

	per := n / k
	shards := make([][]byte, total)

	// Pre-allocate all shard buffers to avoid repeated allocations
	for i := 0; i < total; i++ {
		shards[i] = make([]byte, per)
	}

	for i := 0; i < k; i++ {
		start := i * per
		end := start + per
		if end > n {
			end = n
		}
		if start < n {
			copy(shards[i][:end-start], data[start:end])
		}
	}

	// Apply Reed-Solomon encoding with parallel processing if encoder is available
	if enc, ok := rsEnc.(rs.Encoder); ok {
		// Parallel encoding for better performance
		var wg sync.WaitGroup
		errChan := make(chan error, 1)

		// Encode in parallel using goroutines
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := enc.Encode(shards); err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}()

		// Wait for completion
		wg.Wait()
		close(errChan)

		if err := <-errChan; err != nil {
			log.Printf("rs encode err: %v", err)
			return
		}
	}

	for i := 0; i < total; i++ {
		pkt := &packet.Packet{
			ChunkID:    chunkID,
			PacketID:   uint32(i),
			TotalPkts:  uint32(total),
			ChunkBytes: uint32(originalN),
			Version:    config.HeaderVersion,
			FECType:    config.FECRS,
			K:          uint16(k),
			R:          uint16(r),
			ShardSize:  uint32(per),
			DataPkts:   uint32(k),
			Payload:    shards[i],
		}
		enqueuePacketMulti(pkt, ch, sendQ)
	}
	rb.Put(ch)
}

// enqueuePacketMulti puts packet in send queue
func enqueuePacketMulti(pkt *packet.Packet, ch *buffer.Chunk, sendQ chan<- *buffer.PBuf) {
	b := pkt.Marshal()
	pbuf := buffer.GetPBuf()
	pbuf.Inc()

	// Set the packet data in the PBuf
	// Note: This is a simplified implementation. In a real system,
	// we'd need to properly handle the PBuf data storage
	pbuf.SetData(b)

	select {
	case sendQ <- pbuf:
	default:
		log.Printf("WARNING: sendQ full! Packet may be delayed")
		sendQ <- pbuf
	}

	metrics.PromTxPackets.Inc()
	atomic.AddUint64(&txPackets, 1)
	atomic.AddUint64(&txBytes, uint64(len(b)))
	metrics.PromTxBytes.Add(float64(len(b)))
}

// Global counters for statistics
var txPackets uint64
var txBytes uint64