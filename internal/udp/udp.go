package udp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"streamer/internal/utils"
)

// MultiUDPSender управляет множественными UDP соединениями для отправки
type MultiUDPSender struct {
	conns    []*net.UDPConn
	nextConn uint64 // Для round-robin распределения
}

// NewMultiUDPSender создает множественные UDP соединения для отправки
func NewMultiUDPSender(host string, ports []int) (*MultiUDPSender, error) {
	var conns []*net.UDPConn

	for _, port := range ports {
		addr := fmt.Sprintf("%s:%d", host, port)
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			// Закрываем уже открытые соединения при ошибке
			for _, conn := range conns {
				conn.Close()
			}
			return nil, fmt.Errorf("resolve addr %s: %v", addr, err)
		}

		conn, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			// Закрываем уже открытые соединения при ошибке
			for _, conn := range conns {
				conn.Close()
			}
			return nil, fmt.Errorf("dial UDP %s: %v", addr, err)
		}

		// Увеличиваем буфер отправки
		if err := conn.SetWriteBuffer(16 << 20); err != nil {
			log.Printf("failed to set write buffer for %s: %v", addr, err)
		}

		// Включаем GSO (Generic Segmentation Offload) для UDP
		if file, err := conn.File(); err == nil {
			fd := int(file.Fd())
			// UDP_SEGMENT позволяет отправлять большие пакеты, которые ядро разобьёт на MTU-sized фрагменты
			if err := unix.SetsockoptInt(fd, unix.SOL_UDP, unix.UDP_SEGMENT, 1500); err != nil {
				log.Printf("failed to enable GSO for %s: %v", addr, err)
			}
			file.Close()
		}

		conns = append(conns, conn)
		log.Printf("UDP sender connected to %s", addr)
	}

	return &MultiUDPSender{
		conns:    conns,
		nextConn: 0,
	}, nil
}

// GetNextConn возвращает следующее соединение по round-robin алгоритму
func (ms *MultiUDPSender) GetNextConn() *net.UDPConn {
	index := atomic.AddUint64(&ms.nextConn, 1) % uint64(len(ms.conns))
	return ms.conns[index]
}

// GetConnByIndex возвращает соединение по индексу (для равномерного распределения packets)
func (ms *MultiUDPSender) GetConnByIndex(index uint32) *net.UDPConn {
	return ms.conns[index%uint32(len(ms.conns))]
}

// Close закрывает все соединения
func (ms *MultiUDPSender) Close() error {
	var lastErr error
	for i, conn := range ms.conns {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection %d: %v", i, err)
			lastErr = err
		}
	}
	return lastErr
}

// GetConnections возвращает все соединения для worker'ов
func (ms *MultiUDPSender) GetConnections() []*net.UDPConn {
	return ms.conns
}

// MultiUDPReceiver управляет множественными UDP соединениями для приема
type MultiUDPReceiver struct {
	conns   []*net.UDPConn
	packets chan []byte
	errors  chan error
	wg      sync.WaitGroup
	// ✅ НОВЫЕ поля для корректного shutdown
	ctx     context.Context
	cancel  context.CancelFunc
	closed  int32 // atomic flag для защиты от повторных вызовов Close()
}

// NewMultiUDPReceiver создает множественные UDP соединения для приема
func NewMultiUDPReceiver(host string, ports []int, bufferSize int) (*MultiUDPReceiver, error) {
	var conns []*net.UDPConn
	for _, port := range ports {
		addr := fmt.Sprintf("%s:%d", host, port)
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			// Закрываем уже открытые соединения при ошибке
			for _, conn := range conns {
				conn.Close()
			}
			return nil, fmt.Errorf("resolve addr %s: %v", addr, err)
		}

		var conn *net.UDPConn
		if udpAddr.IP.IsMulticast() {
			conn, err = net.ListenMulticastUDP("udp", nil, udpAddr)
		} else {
			conn, err = net.ListenUDP("udp", udpAddr)
		}
		if err != nil {
			// Закрываем уже открытые соединения при ошибке
			for _, conn := range conns {
				conn.Close()
			}
			return nil, fmt.Errorf("listen UDP %s: %v", addr, err)
		}

		// Включаем SO_REUSEPORT только для multicast
		if udpAddr.IP.IsMulticast() {
			if file, err := conn.File(); err == nil {
				fd := int(file.Fd())
				if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
					log.Printf("failed to set SO_REUSEPORT on %s: %v", addr, err)
				}
				file.Close()
			}
		}

		// Увеличиваем буфер приема до 64MB для лучшей производительности
		if err := conn.SetReadBuffer(64 << 20); err != nil {
			log.Printf("failed to set read buffer for %s: %v", addr, err)
		}

		conns = append(conns, conn)
		log.Printf("UDP receiver listening on %s", addr)
	}

	// ✅ НОВОЕ: Создаем контекст для управления shutdown
	ctx, cancel := context.WithCancel(context.Background())

	receiver := &MultiUDPReceiver{
		conns:   conns,
		packets: make(chan []byte, bufferSize),
		errors:  make(chan error, len(conns)),
		ctx:     ctx,
		cancel:  cancel,
		closed:  0,
	}

	// Запускаем горутины для чтения с каждого порта
	for i, conn := range conns {
		receiver.wg.Add(1)
		go receiver.readLoop(i, conn)
	}

	return receiver, nil
}

// readLoop читает пакеты с одного UDP соединения с корректным завершением
func (mr *MultiUDPReceiver) readLoop(connID int, conn *net.UDPConn) {
	defer mr.wg.Done()
	buf := make([]byte, 64*1024) // Буфер для чтения пакетов

	utils.DebugLog("readLoop %d started", connID)

	for {
		// Читаем пакеты в плотном цикле для максимальной производительности
		utils.DebugLog("readLoop %d: waiting for UDP packet...", connID)
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Проверяем контекст при ошибке
			select {
			case <-mr.ctx.Done():
				utils.DebugLog("readLoop %d: context cancelled on error, exiting", connID)
				return
			default:
			}

			// Проверяем не закрыто ли соединение
			if !errors.Is(err, net.ErrClosed) && mr.ctx.Err() == nil {
				select {
				case mr.errors <- fmt.Errorf("conn %d read error: %v", connID, err):
				default:
				}
			}
			utils.DebugLog("readLoop %d: exiting due to error: %v", connID, err)
			return
		}

		utils.DebugLog("readLoop %d: received UDP packet from %v, size %d", connID, remoteAddr, n)

		// Создаем копию данных для безопасной передачи
		packetData := make([]byte, n)
		copy(packetData, buf[:n])

		// Отправляем пакет с учетом контекста
		select {
		case mr.packets <- packetData:
			utils.DebugLog("readLoop %d: sent packet to channel", connID)
		case <-mr.ctx.Done():
			utils.DebugLog("readLoop %d: context cancelled while sending packet", connID)
			return
		default:
			// Канал заполнен - пакет теряется
			utils.DebugLog("packet buffer full, dropping packet from conn %d", connID)
		}
	}
}

// Packets возвращает канал с входящими пакетами
func (mr *MultiUDPReceiver) Packets() <-chan []byte {
	return mr.packets
}

// Errors возвращает канал с ошибками
func (mr *MultiUDPReceiver) Errors() <-chan error {
	return mr.errors
}

// Close закрывает все соединения и ожидает завершения горутин с таймаутом
func (mr *MultiUDPReceiver) Close() error {
	// ✅ НОВОЕ: Защита от повторных вызовов с помощью atomic операции
	if !atomic.CompareAndSwapInt32(&mr.closed, 0, 1) {
		utils.DebugLog("Close() already called, ignoring")
		return nil // Уже закрыто
	}

	log.Println("[CLIENT] Starting receiver shutdown...")

	// ✅ НОВОЕ: Сначала отменяем контекст для сигнализации readLoop о завершении
	mr.cancel()

	var lastErr error

	// Даем немного времени горутинам для graceful завершения
	time.Sleep(100 * time.Millisecond)

	// Закрываем все соединения
	for i, conn := range mr.conns {
		if err := conn.Close(); err != nil {
			// ✅ ИСПРАВЛЕНО: Не логируем "use of closed network connection" как ошибку
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("failed to close receiver connection %d: %v", i, err)
				lastErr = err
			} else {
				utils.DebugLog("connection %d already closed", i)
			}
		} else {
			utils.DebugLog("connection %d closed successfully", i)
		}
	}

	// ✅ ИСПРАВЛЕНО: Уменьшили таймаут ожидания горутин
	done := make(chan struct{})
	go func() {
		mr.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[CLIENT] All read goroutines finished successfully")
	case <-time.After(2 * time.Second): // Уменьшили с 5 до 2 секунд
		log.Println("[CLIENT] Timeout waiting for read goroutines, forcing shutdown")
	}

	// Закрываем каналы
	close(mr.packets)
	close(mr.errors)

	log.Println("[CLIENT] Receiver shutdown complete")
	return lastErr
}