# Buffer Data Server

The Buffer Data Server handles the data plane of Starburst's Buffer Service, enabling fault-tolerant distributed query execution in Trino. It stores intermediate exchange data (chunks) in memory and spools them to persistent storage when needed.

## Architecture

The Data Server runs as a standalone HTTP server process. It manages the lifecycle of data chunks through the following flow:

```
Trino Worker (addDataPages) → Memory Buffer → Chunk (open → closed → spooled)
Trino Worker (getChunkData) ← Memory/Spooling Storage ← Chunk
```

### Dual HTTP Server Architecture

When `virtual-threads.enabled=true`, the Data Server starts two HTTP servers in the same process:

```
┌──────────────────────────────────────────────────────┐
│ Data Server Process                                  │
│                                                      │
│  ┌────────────────────────────┐                      │
│  │ Main HTTP Server (:8080)   │                      │
│  │  └── DataResource (async)  │                      │
│  └────────────────────────────┘                      │
│                                                      │
│  ┌─────────────────────────────────────┐             │
│  │ VirtualThreads HTTP Server (:8085)  │             │
│  │  └── BlockingDataResource           │             │
│  └─────────────────────────────────────┘             │
│                                                      │
│  Shared: ChunkManager, MemoryAllocator, ...          │
└──────────────────────────────────────────────────────┘
```

Both servers share the same business logic components. The main server uses the async `DataResource`, while the virtual threads server uses `BlockingDataResource` with synchronous I/O. `BufferNodeInfoService` advertises both URIs via `BufferNodeInfo`, and Trino clients can opt in to the virtual threads URI via `exchange.buffer-data.use-virtual-threads-uri`.

When `virtual-threads.enabled=false` (default), only the main HTTP server starts with `DataResource`.

## Configuration

### HTTP Server (Main)

| Property | Default | Description |
|---|---|---|
| `http-server.http.port` | `8080` | HTTP listen port |
| `http-server.threads.max` | `200` | Maximum HTTP server threads |

### Virtual Threads Server (Experimental)

| Property | Default | Description |
|---|---|---|
| `virtual-threads.enabled` | `false` | Enable a second HTTP server using virtual threads and `BlockingDataResource` |
| `virtual-threads.http-server.http.port` | `8085` | HTTP listen port for the virtual threads server |

When enabled, the virtual threads server accepts all standard `http-server.*` properties under the `virtual-threads.` prefix (e.g. `virtual-threads.http-server.threads.max`).

### Trino Client Configuration

| Property | Default | Description |
|---|---|---|
| `exchange.buffer-data.use-virtual-threads-uri` | `false` | Direct Trino workers to use the virtual threads server URI when available |

### Spooling Storage

Storage backend is selected by the URI scheme of `spooling.directory`:

| Scheme | Backend |
|---|---|
| `s3://` | Amazon S3 (or compatible) |
| `gs://` | Google Cloud Storage |
| `abfs://` | Azure Blob Storage |
| `file://` or plain path | Local filesystem (requires `testing.allow-local-spooling=true`) |

Local filesystem spooling is blocked by default and intended only for testing and local development. To enable it, set the hidden property `testing.allow-local-spooling=true` (or `buffer.testing.allow-local-spooling=true` in embedded mode).

## Running in Standalone Mode

### Trino Cluster Configuration

Every Trino node (coordinator and workers) must be configured to use the external buffer service.

`etc/config.properties`:

```properties
retry-policy=TASK
```

`etc/exchange-manager.properties`:

```properties
exchange-manager.name=buffer
exchange.buffer-discovery.uri=http://starburst-buffer-discovery-server:port
# Optional: direct workers to use the virtual threads server on data nodes
exchange.buffer-data.use-virtual-threads-uri=true
```

The `exchange.buffer-discovery.uri` points to the coordinator's main HTTP server, where the Discovery Server API runs and tracks active buffer data nodes.

### Starting the Data Server

**From Command Line:**

```bash
# Build the project
./mvnw clean install -DskipTests

# Set version variable, e.g., for Docker usage
export BUFFER_DATA_SERVER_DOCKER_VERSION=1

# Run the Data Server (using Maven exec plugin - handles classpath automatically)
./mvnw exec:java -pl starburst-buffer-service/starburst-buffer-data-server \
    -Dexec.mainClass=io.starburst.stargate.buffer.data.server.DataServer \
    -Dconfig=starburst-buffer-service/starburst-buffer-data-server/etc/config.properties \
    -Dlog.levels-file=starburst-buffer-service/starburst-buffer-data-server/etc/log.properties
```


**From IntelliJ:**

1. Create a new Run Configuration (Run → Edit Configurations → + → Application)
2. Configure as follows:
    - **Name**: `Data Server (Standalone)`
    - **Main class**: `io.starburst.stargate.buffer.data.server.DataServer`
    - **VM options**: `-ea -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
    - **Working directory**: `$MODULE_DIR$`
    - **Use classpath of module**: `starburst-buffer-data-server`
3. Create `etc/config.properties` in the module directory with the configuration above
4. Run the configuration

Minimal `etc/config.properties`:

```properties
node.id=data-server-1
node.environment=test
http-server.http.port=8090
# Allowing local spooling for local development only - not supported for production
testing.allow-local-spooling=true
spooling.directory=/tmp/trino-buffer-data
discovery-service.uri=http://starburst-buffer-discovery-server:port
```

With virtual threads enabled:

```properties
node.id=data-server-1
node.environment=test
http-server.http.port=8090
virtual-threads.enabled=true
virtual-threads.http-server.http.port=8085
# Allowing local spooling for local development only - not supported for production
testing.allow-local-spooling=true
spooling.directory=/tmp/trino-buffer-data
discovery-service.uri=http://starburst-buffer-discovery-server:port
```

## Running in Embedded Mode

Embedded mode integrates the Data Server directly into the Trino coordinator or worker process, sharing the same HTTP server and port.

### Configuration

Add to `etc/config.properties` in the main Trino configuration:

```properties
# Enable embedded buffer service
embedded-buffer-service-enabled=true

# Buffer configuration
# Allowing local spooling for local development only - not supported for production
testing.allow-local-spooling=true
spooling.directory=/tmp/trino-buffer-data
```

### How It Works

- Data Server runs in the same JVM as Trino
- Shares the same HTTP server (port 8080)
- JAX-RS resources registered on the main server
- No separate HTTP port needed
- Discovery registration uses Trino's main address

### Architecture

```
┌─────────────────────────────────────┐
│ Trino Coordinator/Worker            │
│                                     │
│  ┌───────────────────────────────┐  │
│  │ HTTP Server (port 8080)       │  │
│  │  ├── Trino REST endpoints     │  │
│  │  └── Data Server endpoints    │  │
│  └───────────────────────────────┘  │
│                                     │
│  Data Server Business Logic         │
│  (same Guice injector)              │
└─────────────────────────────────────┘
```

### Use Cases

- Single-node development/testing
- Simplified deployment for small clusters
- Co-located buffer storage with compute
