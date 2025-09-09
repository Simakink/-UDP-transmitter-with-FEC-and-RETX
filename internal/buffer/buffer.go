package buffer

import (
	"container/ring"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rs "github.com/klauspost/reedsolomon"
	"streamer/internal/config"
	"streamer/internal/metrics"
	"streamer/internal/packet"
	"streamer/internal/utils"
)

// PBuf представляет буфер пакета с подсчетом ссылок для memory pooling
// Это критично для производительности при обработке тысяч пакетов в секунду
type PBuf struct {
	b   *[]byte      // Указатель на буфер данных
	ref int32        // Atomic счетчик ссылок
	mu  sync.RWMutex // Mutex для thread-safe доступа к буферу
}

// Bytes возвращает содержимое буфера в thread-safe манере
// Возвращает nil если буфер уже освобожден или не инициализирован
func (p *PBuf) Bytes() []byte {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.b == nil {
		return nil
	}
	return *p.b
}

// Inc увеличивает счетчик ссылок атомарно
// Должен вызываться каждый раз когда создается новая ссылка на буфер
func (p *PBuf) Inc() {
	if p != nil {
		atomic.AddInt32(&p.ref, 1)
	}
}

// Dec уменьшает счетчик ссылок и освобождает буфер когда счетчик достигает 0
// Автоматически возвращает буфер в pool для переиспользования
func (p *PBuf) Dec() {
	if p == nil {
		return
	}
	if atomic.AddInt32(&p.ref, -1) == 0 {
		p.mu.Lock()
		if p.b != nil {
			// Очищаем буфер и возвращаем в pool
			*p.b = (*p.b)[:0]
			packetPool.Put(p.b)
			p.b = nil
		}
		p.mu.Unlock()
	}
}

// SetData устанавливает данные в буфер
func (p *PBuf) SetData(data []byte) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.b == nil {
		p.b = &data
	} else {
		*p.b = append((*p.b)[:0], data...)
	}
}

// packetPool - глобальный pool буферов для минимизации garbage collection
// Каждый буфер имеет начальную емкость 64KB что покрывает большинство случаев
var packetPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64*1024)
		return &b
	},
}

// GetPBuf получает новый PBuf из pool'а
func GetPBuf() *PBuf {
	pb := packetPool.Get().(*[]byte)
	return &PBuf{b: pb, ref: 1}
}

// BufferedFileWriter accumulates data and writes in large blocks for better I/O performance
type BufferedFileWriter struct {
	file        *os.File
	buffer      []byte
	bufSize     int
	position    int
	mu          sync.Mutex
	writeChan   chan []byte // Channel for async writes
	done        chan struct{} // Signal completion
	totalBytes  int64        // Total bytes written
	writeTime   time.Duration // Total time spent writing
	startTime   time.Time    // Start time for metrics
	numWorkers  int          // Number of async write workers
	fileMu      sync.Mutex   // Mutex for file writes to prevent concurrent access
	workersDone sync.WaitGroup // WaitGroup for worker completion
}

// NewBufferedFileWriter creates a new buffered writer with specified buffer size
func NewBufferedFileWriter(filename string, bufferSize int) (*BufferedFileWriter, error) {
	return NewBufferedFileWriterWithWorkers(filename, bufferSize, 1)
}

// NewBufferedFileWriterWithWorkers creates a new buffered writer with specified buffer size and worker count
func NewBufferedFileWriterWithWorkers(filename string, bufferSize int, numWorkers int) (*BufferedFileWriter, error) {
	return NewBufferedFileWriterWithWorkersAndDirectIO(filename, bufferSize, numWorkers, false)
}

// NewBufferedFileWriterWithWorkersAndDirectIO creates a new buffered writer with specified buffer size, worker count, and Direct I/O option
func NewBufferedFileWriterWithWorkersAndDirectIO(filename string, bufferSize int, numWorkers int, directIO bool) (*BufferedFileWriter, error) {
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024 // 4MB default
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}

	var file *os.File
	var err error

	if directIO {
		// Use Direct I/O for bypassing OS cache (Linux only)
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|syscall.O_DIRECT, 0644)
		if err != nil {
			utils.DebugLog("[BufferedWriter] Direct I/O not supported, falling back to standard I/O: %v", err)
			// Fallback to standard I/O
			file, err = os.Create(filename)
			if err != nil {
				return nil, err
			}
		}
	} else {
		file, err = os.Create(filename)
		if err != nil {
			return nil, err
		}
	}

	writer := &BufferedFileWriter{
		file:        file,
		buffer:      make([]byte, bufferSize),
		bufSize:     bufferSize,
		writeChan:   make(chan []byte, 10*numWorkers), // Scale channel buffer with workers
		done:        make(chan struct{}),
		startTime:   time.Now(),
		numWorkers:  numWorkers,
		workersDone: sync.WaitGroup{},
	}

	// Start async write goroutines
	writer.workersDone.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go writer.writeWorker()
	}

	return writer, nil
}

// Write accumulates data in buffer and sends to async writer when full
func (w *BufferedFileWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	totalWritten := 0
	dataLen := len(data)

	for totalWritten < dataLen {
		// Calculate how much we can write to current buffer
		remaining := w.bufSize - w.position
		toWrite := dataLen - totalWritten
		if toWrite > remaining {
			toWrite = remaining
		}

		// Copy data to buffer
		copy(w.buffer[w.position:], data[totalWritten:totalWritten+toWrite])
		w.position += toWrite
		totalWritten += toWrite

		// Send buffer to async writer if full
		if w.position >= w.bufSize {
			if err := w.sendBufferLocked(); err != nil {
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

// writeWorker runs in background to handle async writes
func (w *BufferedFileWriter) writeWorker() {
	defer w.workersDone.Done()
	for {
		select {
		case data, ok := <-w.writeChan:
			if !ok {
				// Channel closed, exit
				return
			}
			// Write data to disk with timing (serialized with mutex)
			w.fileMu.Lock()
			start := time.Now()
			n, err := w.file.Write(data)
			duration := time.Since(start)
			w.fileMu.Unlock()

			if err != nil {
				utils.DebugLog("[BufferedWriter] Write error: %v", err)
			} else {
				// Update metrics
				atomic.AddInt64(&w.totalBytes, int64(n))
				atomic.AddInt64((*int64)(&w.writeTime), int64(duration))
			}
		}
	}
}

// sendBufferLocked sends current buffer to async writer
func (w *BufferedFileWriter) sendBufferLocked() error {
	// Create a copy of the buffer data to avoid race conditions
	bufferCopy := make([]byte, w.position)
	copy(bufferCopy, w.buffer[:w.position])

	// Send to async writer (non-blocking)
	select {
	case w.writeChan <- bufferCopy:
		w.position = 0
		return nil
	default:
		// Channel full, block and wait
		w.writeChan <- bufferCopy
		w.position = 0
		return nil
	}
}

// Flush forces a write of current buffer contents
func (w *BufferedFileWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Send any remaining data in buffer
	if w.position > 0 {
		if err := w.sendBufferLocked(); err != nil {
			return err
		}
	}

	// Wait for all pending writes to complete
	// This is a simple implementation - in production you'd want better sync
	time.Sleep(100 * time.Millisecond)

	return nil
}

// GetWriteSpeed returns the current write speed in MB/s
func (w *BufferedFileWriter) GetWriteSpeed() float64 {
	totalBytes := atomic.LoadInt64(&w.totalBytes)
	writeTimeNano := atomic.LoadInt64((*int64)(&w.writeTime))
	writeTime := time.Duration(writeTimeNano)

	if writeTime.Seconds() > 0 {
		bytesPerSec := float64(totalBytes) / writeTime.Seconds()
		return bytesPerSec / (1024 * 1024)
	}
	return 0
}

// Close flushes remaining data and closes the file
func (w *BufferedFileWriter) Close() error {
	w.mu.Lock()

	// Send any remaining data in buffer
	if w.position > 0 {
		w.sendBufferLocked()
	}

	w.mu.Unlock()

	// Close the write channel to signal workers to exit
	close(w.writeChan)

	// Wait for all workers to finish
	w.workersDone.Wait()

	return w.file.Close()
}

// Chunk представляет набор пакетов, которые составляют логическую единицу данных
// Один chunk обычно содержит фиксированный размер данных (например, 1MB)
type Chunk struct {
	ID        uint64    // Уникальный ID chunk'а
	Timestamp time.Time // Время создания для отслеживания возраста
	Packets   []*PBuf   // Массив пакетов chunk'а (включая FEC пакеты)
	PktCnt    int       // Количество пакетов в chunk'е
}

// RingBuffer хранит последние отправленные chunks для возможности повторной передачи (RETX)
// Использует кольцевую структуру для автоматического удаления старых chunks
type RingBuffer struct {
	mu    sync.RWMutex          // RW mutex для concurrent доступа
	ring  *ring.Ring            // Кольцевая структура для хранения chunks
	index map[uint64]*Chunk     // Индекс для быстрого поиска chunks по ID
}

// NewRingBuffer создает новый ring buffer с заданным размером
// Размер определяет сколько chunks может храниться для RETX
func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 {
		size = 128 // Разумное значение по умолчанию
	}
	return &RingBuffer{
		ring:  ring.New(size),
		index: make(map[uint64]*Chunk),
	}
}

// Put добавляет новый chunk в ring buffer
// Автоматически удаляет самый старый chunk если буфер полон
func (rb *RingBuffer) Put(chunk *Chunk) {
	if chunk == nil {
		return
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Освобождаем ресурсы старого chunk'а если он есть
	if old, ok := rb.ring.Value.(*Chunk); ok && old != nil {
		for _, pb := range old.Packets {
			if pb != nil {
				pb.Dec() // Уменьшаем reference count
			}
		}
		delete(rb.index, old.ID) // Удаляем из индекса
	}

	// Добавляем новый chunk
	rb.ring.Value = chunk
	rb.index[chunk.ID] = chunk
	rb.ring = rb.ring.Next()
}

// Get ищет chunk по ID в ring buffer
// Используется RETX механизмом для повторной отправки потерянных пакетов
func (rb *RingBuffer) Get(id uint64) (*Chunk, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	c, ok := rb.index[id]
	return c, ok
}
// ChunkBuffer буферизует пакеты одного chunk'а до его полной сборки
// Включает логику FEC восстановления и отслеживания готовности
type ChunkBuffer struct {
	ID         uint64                 // ID chunk'а
	Packets    map[uint32][]byte      // Карта пакетов: PacketID -> данные
	Got        int                    // Количество полученных пакетов
	Total      int                    // Общее количество пакетов в chunk'е
	Ready      bool                   // Готов ли chunk для записи в output
	createdAt  time.Time             // Время создания для timeout'ов
	chunkBytes int                   // Размер оригинальных данных chunk'а
	fecType    uint8                 // Тип FEC коррекции
	k, r       int                   // Reed-Solomon параметры (k=data, r=parity)
	shardSize  int                   // Размер одного shard'а
	dataPkts   int                   // Количество data-пакетов (без parity)
	rsShards   [][]byte              // Буфер для Reed-Solomon shard'ов
	rsEnc      rs.Encoder            // Reed-Solomon encoder для восстановления
	rsPresent  int                   // Количество присутствующих RS шардов (кеш)
	rsDone     bool                  // Флаг завершения RS обработки
	fecProcessor func(*ChunkBuffer, *packet.Packet) // Оптимизированный FEC процессор
	mu         sync.Mutex            // Mutex для thread-safety
}

// ReorderBuffer управляет множественными ChunkBuffer'ами и обеспечивает упорядоченную запись
// Это сердце клиентской части - собирает пакеты, применяет FEC, записывает данные по порядку
type ReorderBuffer struct {
	mu             sync.Mutex                          // Главный mutex для всего буфера
	chunks         map[uint64]*ChunkBuffer             // Активные chunks
	expected       uint64                              // ID следующего ожидаемого chunk'а для записи
	writer         io.Writer                           // Куда записывать восстановленные данные
	retxReq        func(chunkID uint64, missing []uint32) // Callback для запроса повторной передачи
	timeout        time.Duration                       // Таймаут ожидания пакетов перед RETX
	// Поля для обработки завершения потока
	endOfStream      bool                  // Получен END маркер
	lastChunkID      uint64               // ID последнего chunk'а
	streamComplete   bool                 // Поток полностью обработан
	completionNotify chan struct{}        // Канал уведомлений о завершении
}

// getFECProcessor возвращает оптимизированный процессор для типа FEC
func (rb *ReorderBuffer) getFECProcessor(fecType uint8) func(*ChunkBuffer, *packet.Packet) {
	switch fecType {
	case config.FECNone:
		return func(cb *ChunkBuffer, pkt *packet.Packet) {
			rb.checkNoneReadiness(cb)
		}
	case config.FECXOR:
		return func(cb *ChunkBuffer, pkt *packet.Packet) {
			rb.tryXORRecovery(cb)
		}
	case config.FECRS:
		return func(cb *ChunkBuffer, pkt *packet.Packet) {
			if cb.rsEnc != nil {
				rb.tryRSRecovery(cb, pkt)
			}
		}
	default:
		return nil
	}
}

// NewReorderBuffer creates a new ReorderBuffer
func NewReorderBuffer(w io.Writer, timeout time.Duration, retx func(uint64, []uint32)) *ReorderBuffer {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond // Разумный timeout по умолчанию
	}

	rb := &ReorderBuffer{
		chunks:           make(map[uint64]*ChunkBuffer),
		expected:         1, // Chunks начинаются с ID=1
		writer:           w,
		retxReq:          retx,
		timeout:          timeout,
		endOfStream:      false,
		lastChunkID:      0,
		streamComplete:   false,
		completionNotify: make(chan struct{}, 1),
	}

	// Start periodic cleanup goroutine
	go rb.periodicCleanup()

	return rb
}

// Add добавляет полученный пакет в соответствующий ChunkBuffer
func (rb *ReorderBuffer) Add(pkt *packet.Packet) {
	utils.DebugLog("[REORDER] Add() called for packet: chunk=%d, packet=%d", pkt.ChunkID, pkt.PacketID)
	metrics.PromRxPackets.Inc() // Увеличиваем счетчик принятых пакетов

	// Обработка END маркеров для graceful shutdown
	if pkt.PacketID == config.EndOfStreamMarker && string(pkt.Payload) == "END_OF_STREAM" {
		utils.DebugLog("[REORDER] INFO: Received END_OF_STREAM marker. Last chunk should be: %d", pkt.ChunkID-1)
		rb.handleEndOfStream(pkt.ChunkID - 1)
		return
	}

	rb.mu.Lock()
	cb, ok := rb.chunks[pkt.ChunkID]
	if !ok {
		// Создаем новый ChunkBuffer для этого chunk'а
		if pkt.ChunkID < rb.expected {
			rb.mu.Unlock()
			utils.DebugLog("[REORDER] WARNING: Ignoring late packet %d for already processed chunk %d (expected: %d)",
				pkt.PacketID, pkt.ChunkID, rb.expected)
			metrics.PromLatePackets.Inc()
			return
		}

		cb = &ChunkBuffer{
			ID:         pkt.ChunkID,
			Packets:    make(map[uint32][]byte),
			Total:      int(pkt.TotalPkts),
			createdAt:  time.Now(),
			chunkBytes: int(pkt.ChunkBytes),
			fecType:    pkt.FECType,
			k:          int(pkt.K),
			r:          int(pkt.R),
			shardSize:  int(pkt.ShardSize),
			dataPkts:   int(pkt.DataPkts),
		}

		// Инициализируем Reed-Solomon encoder если нужен
		if cb.fecType == config.FECRS && cb.k > 0 && cb.r >= 0 {
			if enc, err := rs.New(cb.k, cb.r); err == nil {
				cb.rsEnc = enc
				cb.rsShards = make([][]byte, cb.k+cb.r)
				utils.DebugLog("[REORDER] SUCCESS: Initialized RS encoder for chunk %d (k=%d, r=%d)", pkt.ChunkID, cb.k, cb.r)
			} else {
				utils.DebugLog("[REORDER] ERROR: Failed to initialize RS encoder for chunk %d: %v", pkt.ChunkID, err)
			}
		}

		// Устанавливаем оптимизированный FEC процессор
		cb.fecProcessor = rb.getFECProcessor(cb.fecType)

		rb.chunks[pkt.ChunkID] = cb

		utils.DebugLog("[REORDER] SUCCESS: Created new ChunkBuffer for chunk %d (k=%d r=%d total=%d fecType=%d)",
			pkt.ChunkID, cb.k, cb.r, cb.Total, cb.fecType)
	}
	rb.mu.Unlock() // Разблокируем ReorderBuffer раньше

	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Добавляем пакет если его еще нет
	if _, exists := cb.Packets[pkt.PacketID]; !exists {
		cb.Packets[pkt.PacketID] = append([]byte(nil), pkt.Payload...)
		cb.Got++

		utils.DebugLog("[REORDER] SUCCESS: Chunk %d: received NEW packet %d, total received: %d/%d",
			pkt.ChunkID, pkt.PacketID, cb.Got, cb.Total)
	} else {
		utils.DebugLog("[REORDER] INFO: Chunk %d: duplicate packet %d ignored", pkt.ChunkID, pkt.PacketID)
		return
	}

	// Используем оптимизированный FEC процессор вместо множественных проверок
	if cb.fecProcessor != nil {
		utils.DebugLog("[REORDER] Running FEC processor for chunk %d", pkt.ChunkID)
		cb.fecProcessor(cb, pkt)
	} else {
		utils.DebugLog("[REORDER] WARNING: No FEC processor for chunk %d (fecType=%d)", pkt.ChunkID, cb.fecType)
	}

	utils.DebugLog("[REORDER] Checking RETX timeout for chunk %d", pkt.ChunkID)
	rb.mu.Lock()
	rb.checkRetxTimeout()
	rb.mu.Unlock()
}

// handleEndOfStream обрабатывает окончание потока данных
func (rb *ReorderBuffer) handleEndOfStream(lastChunkID uint64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.endOfStream {
		return
	}

	utils.DebugLog("[CLIENT] End of stream detected. Last chunk: %d, currently expected: %d", lastChunkID, rb.expected)

	rb.endOfStream = true
	rb.lastChunkID = lastChunkID

	// Flush any remaining ready chunks before checking completion
	rb.flushReadyLocked()

	if rb.expected > lastChunkID {
		utils.DebugLog("[CLIENT] All chunks processed. Stream complete.")
		rb.streamComplete = true
		select {
		case rb.completionNotify <- struct{}{}:
		default:
		}
		return
	}

	go rb.waitForCompletion()
}

// waitForCompletion ждет завершения обработки всех chunks
func (rb *ReorderBuffer) waitForCompletion() {
	timeout := time.NewTimer(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer timeout.Stop()
	defer ticker.Stop()

	for {
		select {
		case <-timeout.C:
			utils.DebugLog("[CLIENT] Completion timeout. Expected: %d, Last: %d. Forcing shutdown.",
				rb.expected, rb.lastChunkID)
			rb.mu.Lock()
			rb.streamComplete = true
			rb.mu.Unlock()
			select {
			case rb.completionNotify <- struct{}{}:
			default:
			}
			return

		case <-ticker.C:
			rb.mu.Lock()
			if rb.expected > rb.lastChunkID {
				utils.DebugLog("[CLIENT] All chunks processed. Stream complete.")
				rb.streamComplete = true
				select {
				case rb.completionNotify <- struct{}{}:
				default:
				}
				rb.mu.Unlock()
				return
			}

			utils.DebugLog("[CLIENT] Waiting for completion. Expected: %d, Last: %d, Missing chunks: %d",
				rb.expected, rb.lastChunkID, rb.lastChunkID+1-rb.expected)
			rb.mu.Unlock()
		}
	}
}

// GetCompletionNotify возвращает канал уведомлений о завершении потока
func (rb *ReorderBuffer) GetCompletionNotify() <-chan struct{} {
	return rb.completionNotify
}

// IsComplete проверяет завершен ли поток
func (rb *ReorderBuffer) IsComplete() bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	complete := rb.streamComplete
	utils.DebugLog("[REORDER] IsComplete() called, returning: %v (endOfStream=%v, expected=%d, lastChunkID=%d)",
		complete, rb.endOfStream, rb.expected, rb.lastChunkID)
	return complete
}

// checkNoneReadiness проверяет готовность chunk'а без FEC
func (rb *ReorderBuffer) checkNoneReadiness(cb *ChunkBuffer) {
	utils.DebugLog("NONE: Checking chunk %d: got=%d/%d total=%d dataPkts=%d", cb.ID, cb.Got, cb.Total, cb.Total, cb.dataPkts)

	if cb.Got >= cb.Total {
		cb.Ready = true
		utils.DebugLog("NONE: Chunk %d ready: got all %d packets", cb.ID, cb.Total)
		return
	}

	ready := true
	for i := 0; i < cb.dataPkts; i++ {
		if _, ok := cb.Packets[uint32(i)]; !ok {
			ready = false
			break
		}
	}
	if ready && !cb.Ready {
		cb.Ready = true
		utils.DebugLog("NONE: Chunk %d ready: got all %d data packets", cb.ID, cb.dataPkts)
		rb.flushReadyLocked()
	}
}

// tryXORRecovery пытается восстановить потерянные пакеты с помощью XOR
func (rb *ReorderBuffer) tryXORRecovery(cb *ChunkBuffer) {
	utils.DebugLog("[FEC] XOR: Attempting recovery for chunk %d", cb.ID)
	k := cb.dataPkts
	parityIdx := uint32(k)

	parity, hasParity := cb.Packets[parityIdx]
	if !hasParity {
		utils.DebugLog("[FEC] XOR: No parity packet available for chunk %d", cb.ID)
		return
	}

	var missing []int
	for i := 0; i < k; i++ {
		if _, ok := cb.Packets[uint32(i)]; !ok {
			missing = append(missing, i)
		}
	}

	utils.DebugLog("[FEC] XOR: Chunk %d has %d missing packets: %v", cb.ID, len(missing), missing)

	if len(missing) == 1 {
		utils.DebugLog("[FEC] XOR: Recovering packet %d for chunk %d", missing[0], cb.ID)
		per := cb.shardSize
		rec := make([]byte, per)
		copy(rec, parity[:utils.Min(per, len(parity))])

		for i := 0; i < k; i++ {
			if i == missing[0] {
				continue
			}
			if p, ok := cb.Packets[uint32(i)]; ok {
				for j := 0; j < per && j < len(p) && j < len(rec); j++ {
					rec[j] ^= p[j]
				}
			}
		}

		cb.Packets[uint32(missing[0])] = rec
		cb.Got++
		metrics.PromFECRecovered.Inc()
		// Обновление статистики клиента
		atomic.AddUint64(&utils.GlobalClientStats.RecoveredPackets, 1)
		utils.DebugLog("[FEC] XOR: SUCCESS - recovered packet %d for chunk %d", missing[0], cb.ID)
	} else {
		utils.DebugLog("[FEC] XOR: Cannot recover chunk %d - %d missing packets (need exactly 1)", cb.ID, len(missing))
	}

	ready := true
	for i := 0; i < k; i++ {
		if _, ok := cb.Packets[uint32(i)]; !ok {
			ready = false
			break
		}
	}
	if ready && !cb.Ready { // Only mark as ready
		utils.DebugLog("[FEC] XOR: Chunk %d marked as ready after recovery", cb.ID)
		cb.Ready = true
		rb.flushReadyLocked()
	} else if !ready {
		utils.DebugLog("[FEC] XOR: Chunk %d still not ready after recovery", cb.ID)
	}
}

// tryRSRecovery пытается восстановить с помощью Reed-Solomon
func (rb *ReorderBuffer) tryRSRecovery(cb *ChunkBuffer, pkt *packet.Packet) {
	utils.DebugLog("[FEC] RS: Processing chunk %d, packet %d, rsDone=%v, rsPresent=%d/%d",
		cb.ID, pkt.PacketID, cb.rsDone, cb.rsPresent, cb.k+cb.r)

	// Пропускаем если RS обработка уже завершена
	if cb.rsDone {
		utils.DebugLog("[FEC] RS: Skipping chunk %d - RS processing already done", cb.ID)
		return
	}

	// Проверяем, что rsEnc инициализирован
	if cb.rsEnc == nil {
		utils.DebugLog("[FEC] RS: ERROR - rsEnc not initialized for chunk %d", cb.ID)
		return
	}

	// Добавляем новый шард если это RS пакет
	if int(pkt.PacketID) < cb.k+cb.r {
		per := cb.shardSize
		sh := make([]byte, per)
		copy(sh, pkt.Payload[:utils.Min(per, len(pkt.Payload))])
		if cb.rsShards[pkt.PacketID] == nil {
			cb.rsPresent++ // Увеличиваем кеш только если шард был пустым
			utils.DebugLog("[FEC] RS: Added NEW shard %d for chunk %d, present=%d/%d",
				pkt.PacketID, cb.ID, cb.rsPresent, cb.k+cb.r)
		} else {
			utils.DebugLog("[FEC] RS: Updated existing shard %d for chunk %d", pkt.PacketID, cb.ID)
		}
		cb.rsShards[pkt.PacketID] = sh
	} else {
		utils.DebugLog("[FEC] RS: Packet %d is not an RS shard for chunk %d", pkt.PacketID, cb.ID)
	}

	// Проверяем готовность только если достаточно шардов
	if cb.rsPresent >= cb.k {
		utils.DebugLog("[FEC] RS: Attempting reconstruction for chunk %d (%d shards present)", cb.ID, cb.rsPresent)
		// Используем существующие шарды без копирования
		if err := cb.rsEnc.Reconstruct(cb.rsShards); err == nil {
			recovered := 0
			for i := 0; i < cb.k; i++ {
				if _, ok := cb.Packets[uint32(i)]; !ok && cb.rsShards[i] != nil {
					cb.Packets[uint32(i)] = append([]byte(nil), cb.rsShards[i]...)
					cb.Got++
					recovered++
					utils.DebugLog("[FEC] RS: Recovered data packet %d for chunk %d", i, cb.ID)
				}
			}
			if recovered > 0 {
				metrics.PromFECRecovered.Add(float64(recovered))
				atomic.AddUint64(&utils.GlobalClientStats.RecoveredPackets, uint64(recovered))
				utils.DebugLog("[FEC] RS: SUCCESS - recovered %d packets for chunk %d", recovered, cb.ID)
			} else {
				utils.DebugLog("[FEC] RS: Reconstruction successful but no new packets recovered for chunk %d", cb.ID)
			}
			cb.rsDone = true // Помечаем как завершенную
			cb.Ready = true // Отмечаем как готовый
			// Safe to call flushReadyLocked here since we're not inside it
			rb.flushReadyLocked()
			utils.DebugLog("[FEC] RS: Chunk %d marked as ready after recovery", cb.ID)
		} else {
			utils.DebugLog("[FEC] RS: ERROR - reconstruction failed for chunk %d: %v", cb.ID, err)
		}
	} else {
		utils.DebugLog("[FEC] RS: Not enough shards for chunk %d (%d/%d)", cb.ID, cb.rsPresent, cb.k)
	}
}

// checkRetxTimeout проверяет нужно ли послать RETX запрос
// Предполагается, что rb.mu уже заблокирован вызывающим кодом
func (rb *ReorderBuffer) checkRetxTimeout() {
	exp, ok := rb.chunks[rb.expected]

	if !ok {
		// If chunk doesn't exist but we expect it, trigger RETX for missing chunk
		if rb.retxReq != nil && (!rb.endOfStream || rb.expected <= rb.lastChunkID) {
			utils.DebugLog("[RETX] Chunk %d missing, requesting all packets (endOfStream=%v, lastChunkID=%d)", rb.expected, rb.endOfStream, rb.lastChunkID)
			rb.retxReq(rb.expected, []uint32{0}) // Request packet 0 to trigger chunk creation
			metrics.PromRetxRequests.Inc()
		} else {
			utils.DebugLog("[RETX] Skipping RETX for missing chunk %d (retxReq=%v, endOfStream=%v, expected<=%d: %v)", rb.expected, rb.retxReq != nil, rb.endOfStream, rb.lastChunkID, rb.expected <= rb.lastChunkID)
		}
		return
	}

	if !exp.Ready && time.Since(exp.createdAt) > rb.timeout {
		utils.DebugLog("[RETX] Timeout reached for chunk %d (age: %v, timeout: %v)", rb.expected, time.Since(exp.createdAt), rb.timeout)
		var missing []uint32

		exp.mu.Lock()
		switch exp.fecType {
		case config.FECRS:
			total := exp.k + exp.r
			if exp.rsShards != nil && len(exp.rsShards) == total {
				// Используем кеш для быстрой проверки
				for i := 0; i < total; i++ {
					if exp.rsShards[i] == nil {
						missing = append(missing, uint32(i))
					}
				}
			} else {
				for i := 0; i < total; i++ {
					if _, ok := exp.Packets[uint32(i)]; !ok {
						missing = append(missing, uint32(i))
					}
				}
			}
		case config.FECXOR:
			for i := 0; i <= exp.dataPkts; i++ {
				if _, ok := exp.Packets[uint32(i)]; !ok {
					missing = append(missing, uint32(i))
				}
			}
		default:
			for i := 0; i < exp.Total; i++ {
				if _, ok := exp.Packets[uint32(i)]; !ok {
					missing = append(missing, uint32(i))
				}
			}
		}
		exp.mu.Unlock()

		if len(missing) > 0 && rb.retxReq != nil {
			utils.DebugLog("[RETX] Sending RETX request for chunk %d, missing %d packets: %v", rb.expected, len(missing), missing)
			rb.retxReq(rb.expected, missing)
			metrics.PromRetxRequests.Inc()
			exp.createdAt = time.Now()
			utils.DebugLog("[RETX] Reset timeout for chunk %d", rb.expected)
		} else {
			utils.DebugLog("[RETX] Not sending RETX for chunk %d (missing=%d, retxReq=%v)", rb.expected, len(missing), rb.retxReq != nil)
		}
	} else if exp.Ready {
		utils.DebugLog("[RETX] Chunk %d is ready, no RETX needed", rb.expected)
	} else {
		utils.DebugLog("[RETX] Chunk %d not ready but timeout not reached (age: %v)", rb.expected, time.Since(exp.createdAt))
	}
}

// flushReadyLocked обрабатывает все готовые чанки последовательно для поддержания порядка
func (rb *ReorderBuffer) flushReadyLocked() {
	utils.DebugLog("[REORDER] FLUSH: Starting flush, expected=%d, total chunks=%d, endOfStream=%v", rb.expected, len(rb.chunks), rb.endOfStream)

	for {
		cb, ok := rb.chunks[rb.expected]
		if !ok {
			// Check if we've reached the end of stream
			if rb.endOfStream && rb.expected > rb.lastChunkID {
				utils.DebugLog("[REORDER] FLUSH: Reached end of stream at chunk %d (last=%d), stopping flush", rb.expected, rb.lastChunkID)
				return
			}

			// Chunk not found - this could be a gap that needs RETX
			utils.DebugLog("[REORDER] FLUSH: Chunk %d not found, checking for gaps", rb.expected)

			// If we have end-of-stream info, check if this chunk should exist
			if rb.endOfStream && rb.expected <= rb.lastChunkID {
				utils.DebugLog("[REORDER] FLUSH: Gap detected at chunk %d (expected <= %d), triggering RETX", rb.expected, rb.lastChunkID)
				// Trigger RETX for this missing chunk
				rb.triggerRetxForMissingChunk(rb.expected)
				return // Stop flush until RETX completes
			}

			// If we don't have end-of-stream yet, just wait
			utils.DebugLog("[REORDER] FLUSH: Chunk %d not found but end-of-stream not received yet, stopping flush", rb.expected)
			return
		}
		if !cb.Ready {
			utils.DebugLog("[REORDER] FLUSH: Chunk %d not ready (%d/%d packets, ready=%v), stopping flush",
				rb.expected, cb.Got, cb.Total, cb.Ready)
			return
		}

		utils.DebugLog("[REORDER] FLUSH: Processing READY chunk %d (%d/%d packets, ready=%v)",
			rb.expected, cb.Got, cb.Total, cb.Ready)

		// Пишем данные чанка напрямую
		rb.writeChunkDataDirect(cb)

		// Освобождаем чанк из памяти
		delete(rb.chunks, rb.expected)
		utils.DebugLog("[REORDER] FLUSH: Deleted chunk %d from buffer", rb.expected)
		rb.expected++
		utils.DebugLog("[REORDER] FLUSH: Next expected chunk: %d", rb.expected)
	}
}

// periodicCleanup runs in background to handle edge cases and cleanup
func (rb *ReorderBuffer) periodicCleanup() {
	ticker := time.NewTicker(1 * time.Second) // Run every 1 second for faster RETX
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			utils.DebugLog("[REORDER] Periodic cleanup running...")
			rb.mu.Lock()
			rb.flushReadyLocked() // Periodic flush to handle any ready chunks
			rb.checkRetxTimeout() // Check for RETX timeouts

			// Additional check: if we have gaps in the expected sequence, trigger RETX
			if rb.endOfStream && rb.expected <= rb.lastChunkID {
				_, exists := rb.chunks[rb.expected]
				if !exists {
					utils.DebugLog("[REORDER] CLEANUP: Gap detected at chunk %d during periodic check", rb.expected)
					rb.triggerRetxForMissingChunk(rb.expected)
				}
			}

			rb.mu.Unlock()
		}
	}
}



// writeChunkDataDirect записывает данные чанка в output напрямую
func (rb *ReorderBuffer) writeChunkDataDirect(cb *ChunkBuffer) {
	utils.DebugLog("[WRITE] Starting write for chunk %d", cb.ID)

	dataCount := cb.dataPkts
	written := 0
	limit := cb.chunkBytes

	var allData []byte
	for i := 0; i < dataCount && written < limit; i++ {
		if p, ok := cb.Packets[uint32(i)]; ok {
			toWrite := utils.Min(len(p), limit-written)
			if toWrite > 0 {
				allData = append(allData, p[:toWrite]...)
				written += toWrite
				utils.DebugLog("[WRITE] Added %d bytes from packet %d to chunk %d", toWrite, i, cb.ID)
			}
		} else {
			utils.DebugLog("[WRITE] ERROR: Missing data packet %d in chunk %d", i, cb.ID)
		}
	}

	utils.DebugLog("[WRITE] Collected %d bytes for chunk %d (expected %d)", len(allData), cb.ID, limit)

	if len(allData) > 0 {
		n, err := rb.writer.Write(allData)
		if err != nil {
			utils.DebugLog("[WRITE] ERROR: Write error for chunk %d: %v", cb.ID, err)
			return
		}
		utils.DebugLog("[WRITE] SUCCESS: Wrote %d bytes to output for chunk %d", n, cb.ID)
	} else {
		utils.DebugLog("[WRITE] WARNING: No data to write for chunk %d", cb.ID)
	}

	if written != limit {
		utils.DebugLog("[WRITE] WARNING: Chunk %d incomplete: written %d, expected %d", cb.ID, written, limit)
	} else {
		utils.DebugLog("[WRITE] SUCCESS: Chunk %d completed: written %d bytes", cb.ID, written)
	}

	// Обновление статистики клиента
	atomic.AddUint64(&utils.GlobalClientStats.TotalChunks, 1)
	utils.DebugLog("[WRITE] Updated stats: total chunks now %d", atomic.LoadUint64(&utils.GlobalClientStats.TotalChunks))
}


// FeedRetxPacket добавляет пакет полученный через RETX
func (rb *ReorderBuffer) FeedRetxPacket(pkt *packet.Packet) {
	rb.Add(pkt)
}

// triggerRetxForMissingChunk triggers RETX for a specific missing chunk
func (rb *ReorderBuffer) triggerRetxForMissingChunk(chunkID uint64) {
	utils.DebugLog("RETX: Triggering RETX for missing chunk %d", chunkID)

	// Create a ChunkBuffer for the missing chunk to track RETX timeout
	cb := &ChunkBuffer{
		ID:         chunkID,
		Packets:    make(map[uint32][]byte),
		Total:      1, // We don't know the actual total, assume 1 for RETX
		createdAt:  time.Now(),
		chunkBytes: 0, // Unknown
		fecType:    config.FECNone, // Assume no FEC for RETX
		k:          1,
		r:          0,
		shardSize:  0,
		dataPkts:   1,
	}

	rb.chunks[chunkID] = cb
	utils.DebugLog("RETX: Created placeholder ChunkBuffer for chunk %d", chunkID)

	// Immediately trigger RETX timeout check
	rb.checkRetxTimeout()
}