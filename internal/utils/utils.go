package utils

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"streamer/internal/config"
)

// debugLog выводит debug сообщения только если включен режим отладки
func DebugLog(format string, args ...interface{}) {
	if config.DebugEnabled {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// setupGracefulShutdown настраивает обработку сигналов завершения
func SetupGracefulShutdown() <-chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	return sigChan
}

// parsePorts парсит строку портов в формате "5000,5001,5003-5005"
// Поддерживает отдельные порты через запятую и диапазоны через тире
func ParsePorts(portStr string) ([]int, error) {
	var ports []int
	parts := strings.Split(portStr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Проверяем диапазон портов (5000-5010)
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid port range: %s", part)
			}

			start, err := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			if err != nil {
				return nil, fmt.Errorf("invalid start port: %s", rangeParts[0])
			}

			end, err := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err != nil {
				return nil, fmt.Errorf("invalid end port: %s", rangeParts[1])
			}

			if start > end {
				return nil, fmt.Errorf("start port %d > end port %d", start, end)
			}

			// Добавляем все порты в диапазоне
			for port := start; port <= end; port++ {
				ports = append(ports, port)
			}
		} else {
			// Одиночный порт
			port, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid port: %s", part)
			}
			ports = append(ports, port)
		}
	}

	if len(ports) == 0 {
		return nil, fmt.Errorf("no valid ports specified")
	}

	// Проверяем валидность портов
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("port %d out of valid range (1-65535)", port)
		}
	}

	return ports, nil
}

// parseAddressWithPorts парсит адрес с портами "host:port1,port2,port3-port5"
func ParseAddressWithPorts(addr string) (string, []int, error) {
	// Разделяем host:ports
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid address format, expected host:ports")
	}

	host := parts[0]
	portStr := parts[1]

	ports, err := ParsePorts(portStr)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse ports: %v", err)
	}

	return host, ports, nil
}

// max возвращает максимальное из двух чисел
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// min возвращает минимальное из двух чисел
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

var GlobalClientStats ClientStats