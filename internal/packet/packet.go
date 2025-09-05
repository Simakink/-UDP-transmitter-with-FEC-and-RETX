package packet

import (
	"encoding/binary"
	"fmt"

	"streamer/internal/config"
)

// Packet представляет UDP пакет в протоколе передачи
// Каждый пакет содержит часть chunk'а данных плюс метаинформацию для восстановления
type Packet struct {
	ChunkID    uint64 // Уникальный ID chunk'а (начинается с 1)
	PacketID   uint32 // ID пакета внутри chunk'а (начинается с 0)
	TotalPkts  uint32 // Общее количество пакетов в chunk'е (включая FEC)
	ChunkBytes uint32 // Размер исходных данных chunk'а в байтах
	Version    uint8  // Версия протокола
	FECType    uint8  // Тип FEC коррекции (FECNone/FECXOR/FECRS)
	K          uint16 // Количество data-пакетов для Reed-Solomon
	R          uint16 // Количество parity-пакетов для Reed-Solomon
	_          uint16 // Резерв для будущих расширений
	ShardSize  uint32 // Размер одного shard'а (пакета) в байтах
	DataPkts   uint32 // Количество пакетов с данными (без FEC)
	Payload    []byte // Полезная нагрузка пакета
}

// Marshal сериализует пакет в байтовый массив для передачи по UDP
// Использует big-endian для совместимости между архитектурами
func (p *Packet) Marshal() []byte {
	buf := make([]byte, config.PacketHeaderLenV1+len(p.Payload))

	// Размещаем поля в фиксированных позициях для надежного декодирования
	binary.BigEndian.PutUint64(buf[0:8], p.ChunkID)
	binary.BigEndian.PutUint32(buf[8:12], p.PacketID)
	binary.BigEndian.PutUint32(buf[12:16], p.TotalPkts)
	binary.BigEndian.PutUint32(buf[16:20], p.ChunkBytes)

	// Version и FECType размещены в отдельных байтах для избежания коллизий
	buf[20] = p.Version
	buf[21] = p.FECType

	// FEC параметры для Reed-Solomon
	binary.BigEndian.PutUint16(buf[22:24], p.K)
	binary.BigEndian.PutUint16(buf[24:26], p.R)
	// [26:28] reserved - резерв для будущих полей
	binary.BigEndian.PutUint32(buf[28:32], p.ShardSize)
	binary.BigEndian.PutUint32(buf[32:36], p.DataPkts)

	// Копируем полезную нагрузку
	copy(buf[config.PacketHeaderLenV1:], p.Payload)
	return buf
}

// UnmarshalPacket десериализует байтовый массив обратно в структуру Packet
// Включает валидацию версии и типа FEC для предотвращения ошибок
func UnmarshalPacket(b []byte) (*Packet, error) {
	if len(b) < config.PacketHeaderLenV1 {
		return nil, fmt.Errorf("packet too short: got %d, need %d", len(b), config.PacketHeaderLenV1)
	}

	p := &Packet{
		ChunkID:    binary.BigEndian.Uint64(b[0:8]),
		PacketID:   binary.BigEndian.Uint32(b[8:12]),
		TotalPkts:  binary.BigEndian.Uint32(b[12:16]),
		ChunkBytes: binary.BigEndian.Uint32(b[16:20]),
		Version:    b[20], // Теперь не перекрывается с другими полями
		FECType:    b[21], // Отдельный байт для типа FEC
		K:          binary.BigEndian.Uint16(b[22:24]),
		R:          binary.BigEndian.Uint16(b[24:26]),
		ShardSize:  binary.BigEndian.Uint32(b[28:32]),
		DataPkts:   binary.BigEndian.Uint32(b[32:36]),
		Payload:    append([]byte(nil), b[config.PacketHeaderLenV1:]...),
	}

	// Валидация для предотвращения обработки несовместимых пакетов
	if p.Version != config.HeaderVersion {
		return nil, fmt.Errorf("unsupported version: %d", p.Version)
	}
	if p.FECType > config.FECRS {
		return nil, fmt.Errorf("unsupported FEC type: %d", p.FECType)
	}

	return p, nil
}