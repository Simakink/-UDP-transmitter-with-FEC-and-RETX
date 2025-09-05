package config

// Константы для типов FEC (Forward Error Correction)
const (
	FECNone uint8 = 0 // Без коррекции ошибок
	FECXOR  uint8 = 1 // XOR коррекция (может восстановить 1 потерянный пакет)
	FECRS   uint8 = 2 // Reed-Solomon коррекция (может восстановить до R потерянных пакетов)

	HeaderVersion     = 1  // Версия протокола пакетов
	PacketHeaderLenV1 = 36 // Размер заголовка пакета версии 1

	// Константы для graceful shutdown
	EndOfStreamMarker = 0xFFFFFFFF // Специальный PacketID для маркера конца потока
)

// Глобальная переменная для управления debug логированием
var DebugEnabled bool