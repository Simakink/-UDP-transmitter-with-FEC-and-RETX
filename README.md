# UDP Streamer with FEC and RETX

A high-performance UDP-based file streaming system with Forward Error Correction (FEC) and Retransmission (RETX) capabilities for reliable data transfer over unreliable networks.

## Features

- **High-Performance UDP Streaming**: Optimized for maximum throughput using multiple ports and advanced I/O techniques
- **Forward Error Correction (FEC)**: Supports multiple FEC types:
  - None: Basic UDP streaming
  - XOR: Simple parity-based recovery (recovers 1 lost packet per chunk)
  - Reed-Solomon: Advanced erasure coding (configurable data/parity shards)
- **Automatic Retransmission (RETX)**: TCP-based control channel for requesting and recovering lost packets
- **Multi-Port Support**: Distributes traffic across multiple UDP ports for increased bandwidth
- **Multicast Support**: Efficient one-to-many streaming using UDP multicast
- **Buffered I/O**: Asynchronous buffered file writing for optimal disk performance
- **Prometheus Metrics**: Built-in monitoring and performance metrics
- **Graceful Shutdown**: Proper cleanup and completion detection
- **Linux io_uring**: High-performance I/O backend for Linux systems

## Architecture

### Components

- **Server**: Reads input file, applies FEC encoding, sends UDP packets
- **Client**: Receives UDP packets, performs FEC recovery, requests RETX for missing data
- **Ring Buffer**: Server-side storage for recent chunks to enable RETX
- **Reorder Buffer**: Client-side buffer for handling out-of-order packets
- **Control Channel**: TCP connection for RETX requests and responses

### Data Flow

```
Input File → Server → FEC Encoding → UDP Packets → Network → Client → FEC Recovery → RETX Requests → Output File
                      ↑                                                            ↓
               Ring Buffer ← RETX Responses ← TCP Control ← Server RETX Processing
```

## Installation

### Prerequisites

- Go 1.22.12 or later
- Linux (recommended for io_uring support)
- For Reed-Solomon FEC: sufficient memory for encoding/decoding

### Build

```bash
go mod download
go build -o streamer .
```

## Usage

### Server Mode

Send a file with Reed-Solomon FEC:

```bash
./streamer server \
  --addr 239.0.0.1:5000-5003 \
  --ctrl :6000 \
  --input /path/to/file \
  --fec rs \
  --fec-k 3 \
  --fec-r 1 \
  --chunk-bytes 1048576 \
  --workers 4
```

### Client Mode

Receive and save a file:

```bash
./streamer client \
  --addr 239.0.0.1:5000-5003 \
  --ctrl 127.0.0.1:6000 \
  --output /path/to/output \
  --buffer-size 134217728 \
  --workers 4
```

## Command Line Options

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `--addr` | 239.0.0.1:5000 | UDP destination (supports multiple ports: host:port1,port2,port3-port5) |
| `--ctrl` | :6000 | TCP control listen address |
| `--chunk-bytes` | 1048576 | Bytes per chunk (before split to packets) |
| `--fec` | none | FEC type (none/xor/rs) |
| `--fec-k` | 3 | Reed-Solomon data shards |
| `--fec-r` | 1 | Reed-Solomon parity shards |
| `--ring` | 60 | Ring buffer seconds |
| `--input` | - | Input file, '-' = stdin |
| `--workers` | 2 | Send workers for std backend |
| `--pool` | 8192 | Initial packet buffer pool size |
| `--interval` | 200 | ms between chunks |
| `--packet-payload` | 1200 | UDP payload per packet (bytes) |
| `--metrics-addr` | :9100 | Prometheus listen address |
| `--debug` | false | Enable debug logging |
| `--grace-period` | 30 | Grace period for server shutdown (seconds) |

### Client Options

| Option | Default | Description |
|--------|---------|-------------|
| `--addr` | 239.0.0.1:5000 | UDP source (supports multiple ports) |
| `--ctrl` | 127.0.0.1:6000 | TCP control to server |
| `--output` | - | Output file ('-' = stdout) |
| `--retx-timeout` | 200 | ms to wait before RETX request |
| `--debug` | false | Enable debug logging |
| `--buffer-size` | 134217728 | Disk write buffer size in bytes |
| `--workers` | 4 | Number of async write workers |
| `--direct-io` | false | Use Direct I/O (Linux only) |

## FEC Configuration

### No FEC
- **Use case**: Reliable networks, maximum performance
- **Overhead**: 0%
- **Recovery**: None (packet loss causes data loss)

### XOR FEC
- **Use case**: Low-loss networks
- **Overhead**: ~0.5% (1 parity packet per chunk)
- **Recovery**: Can recover 1 lost packet per chunk

### Reed-Solomon FEC
- **Use case**: Unreliable networks, high reliability requirements
- **Parameters**:
  - `k`: Number of data shards
  - `r`: Number of parity shards
- **Overhead**: r/(k+r) * 100%
- **Recovery**: Can recover up to r lost packets per chunk
- **Example**: k=3, r=1 → 25% overhead, recovers 1 lost packet

## Performance Optimization

### Network Optimization
- **Multi-port distribution**: Spread traffic across multiple UDP ports
- **Large send/receive buffers**: 16MB UDP buffers
- **UDP_SEGMENT**: Kernel-level packet segmentation
- **SO_REUSEPORT**: Efficient multi-port handling

### I/O Optimization
- **BufferedFileWriter**: 128MB async write buffers
- **Direct I/O**: Bypass OS cache for large files
- **Memory pooling**: Reuse packet buffers to reduce GC pressure
- **io_uring**: High-performance async I/O on Linux

### FEC Optimization
- **Chunk sizing**: Balance between FEC efficiency and memory usage
- **RS parameters**: k=3, r=1 recommended for most use cases
- **Parallel processing**: Concurrent FEC encoding/decoding

## Monitoring

### Prometheus Metrics

The server exposes Prometheus metrics at `:9100/metrics`:

- `udp_tx_packets_total`: Total packets transmitted
- `udp_tx_bytes_total`: Total bytes transmitted
- `udp_rx_packets_total`: Total packets received
- `udp_rx_bytes_total`: Total bytes received
- `retx_requests_total`: Total retransmission requests
- `fec_recovered_packets_total`: Packets recovered via FEC
- `active_ports`: Number of active UDP ports

### Built-in Statistics

Both server and client print comprehensive statistics on completion:

```
[SERVER] === Transmission Statistics ===
[SERVER] Duration: 45.23s
[SERVER] Total chunks: 1024
[SERVER] Total bytes: 1073741824 (1.00 GB)
[SERVER] Network speed: 23.67 MB/s
[SERVER] RETX requests: 15
[SERVER] FEC type: rs
[SERVER] RS parameters: k=3, r=1

[CLIENT] === Reception Statistics ===
[CLIENT] Duration: 45.67s
[CLIENT] Total chunks: 1024
[CLIENT] Total bytes: 1073741824 (1.00 GB)
[CLIENT] Network speed: 23.51 MB/s
[CLIENT] Disk write speed: 89.34 MB/s
[CLIENT] Lost packets: 45
[CLIENT] Recovered packets: 42
[CLIENT] FEC efficiency: 93.3%
```

## Examples

### Basic File Transfer

Server:
```bash
./streamer server --input large_file.dat --addr 192.168.1.100:5000
```

Client:
```bash
./streamer client --output received_file.dat --addr 192.168.1.100:5000
```

### High-Reliability Streaming

Server with RS FEC:
```bash
./streamer server \
  --input video.mp4 \
  --addr 239.0.0.1:5000-5007 \
  --fec rs \
  --fec-k 5 \
  --fec-r 2 \
  --workers 8
```

Client:
```bash
./streamer client \
  --output video.mp4 \
  --addr 239.0.0.1:5000-5007 \
  --buffer-size 268435456 \
  --workers 8
```

### Multicast Streaming

Server:
```bash
./streamer server --addr 239.1.1.1:5000 --input stream_data
```

Multiple clients:
```bash
./streamer client --addr 239.1.1.1:5000 --output client1_output
./streamer client --addr 239.1.1.1:5000 --output client2_output
```

## Troubleshooting

### Common Issues

1. **High packet loss**
   - Increase FEC redundancy (`--fec-r`)
   - Reduce chunk size (`--chunk-bytes`)
   - Use more ports for distribution

2. **Slow disk I/O**
   - Increase buffer size (`--buffer-size`)
   - Use Direct I/O (`--direct-io`)
   - Add more write workers (`--workers`)

3. **Memory usage**
   - Reduce ring buffer size (`--ring`)
   - Decrease packet pool size (`--pool`)
   - Use smaller chunks

4. **Network congestion**
   - Spread across more ports
   - Reduce packet payload size
   - Implement traffic shaping

### Debug Mode

Enable verbose logging:
```bash
./streamer server --debug ...
./streamer client --debug ...
```

## Dependencies

- `github.com/klauspost/reedsolomon`: Reed-Solomon FEC implementation
- `github.com/prometheus/client_golang`: Metrics collection
- `github.com/spf13/cobra`: CLI framework
- `github.com/iceber/iouring-go`: Linux io_uring support
- `golang.org/x/sys/unix`: System calls for optimization

## License

This project is open source. See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Performance Benchmarks

Typical performance on modern hardware:

- **Network speed**: 100-500 MB/s (depending on network and FEC settings)
- **Disk I/O**: 200-1000 MB/s (with buffered writes)
- **Memory usage**: 100-500 MB (depending on ring buffer size)
- **CPU usage**: 10-30% (single core, varies with FEC complexity)

For optimal performance:
- Use Linux with io_uring support
- Configure appropriate FEC parameters for your network conditions
- Use multiple ports for bandwidth aggregation
- Tune buffer sizes based on your storage subsystem