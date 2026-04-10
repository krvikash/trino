# Parquet Reading in Spark Rapids - Comprehensive Guide

**Document Purpose**: Complete technical reference for how NVIDIA Spark Rapids reads Parquet files with GPU acceleration  
**Last Updated**: 2026-04-10  
**Target Audience**: Developers, Claude agents, performance engineers, contributors

---

## Table of Contents

1. [Introduction](#introduction)
2. [Overview](#overview)
3. [Architecture](#architecture)
4. [The Fabricated Mini-Parquet Innovation](#the-fabricated-mini-parquet-innovation)
5. [Reading Pipeline](#reading-pipeline)
6. [Compression Codec Support](#compression-codec-support)
7. [Configuration Reference](#configuration-reference)
8. [Performance Tuning](#performance-tuning)
9. [Code Locations](#code-locations)
10. [Integration with Spark](#integration-with-spark)
11. [Troubleshooting](#troubleshooting)

---

## Introduction

Code is available at https://github.com/NVIDIA/spark-rapids
and may also be checked out at ../spark-rapids
Agents should prefer local checkout, if available.

## Overview

Spark Rapids accelerates Parquet file reading by:
1. **Fabricating optimized mini-Parquet files** in host memory containing only needed data
2. **GPU-native decompression** via cuDF for massively parallel processing
3. **Columnar processing** that avoids row materialization overhead
4. **Intelligent metadata filtering** using native C++ parsers

### Key Differentiators from CPU Spark

| Aspect | CPU Spark | GPU Spark Rapids |
|--------|-----------|------------------|
| **File Access** | Reads directly from original file | Fabricates optimized mini-Parquet in host memory |
| **Column Pruning** | Skips columns during read | Extracts only needed columns into new file |
| **Processing** | Row-oriented batching | GPU-native columnar format |
| **Decompression** | CPU during row materialization | GPU parallel decompression (or optional CPU) |
| **Metadata Parsing** | Java ParquetFileReader | Native C++ footer parsing |
| **Schema Evolution** | Row-by-row type conversions | Vectorized GPU casts |
| **Small Files** | One partition per file | Optional coalescing across files |

---

## Architecture

### High-Level Flow

```
┌─────────────┐
│ Parquet File│ (Original: 1GB, 100 columns, 1000 row groups)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│ 1. Footer Parsing & Metadata Filtering                  │
│    - Native C++ parser via cuDF JNI                     │
│    - Filter row groups by partition predicates          │
│    - Extract metadata for requested columns only        │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Schema Clipping                                      │
│    - Prune unnecessary columns from Parquet schema      │
│    - Handle case sensitivity and field ID matching      │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 3. Fabricate Mini-Parquet in Host Memory               │
│    - Allocate host buffer (exact size calculated)      │
│    - Write "PAR1" magic header                         │
│    - Copy only needed column chunks from row groups    │
│    - Optional: CPU decompress (Snappy/ZSTD)           │
│    - Recompute metadata with new offsets               │
│    - Write updated footer                              │
│    - Write footer size + "PAR1" trailer                │
│    Result: 5MB optimized file (vs 1GB original)        │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 4. GPU Transfer & Decode                                │
│    - Transfer fabricated file to GPU                    │
│    - cuDF reads as valid Parquet file                   │
│    - GPU decompresses in parallel (if compressed)       │
│    - Produces GPU-resident columnar batches             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Post-Processing                                      │
│    - DateTime rebasing (Julian ↔ Gregorian)            │
│    - Schema evolution with vectorized GPU casts         │
│    - Type conversions (unsigned → signed, etc.)         │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
         ┌─────────────────┐
         │ ColumnarBatch   │ (GPU-resident, ready for query processing)
         └─────────────────┘
```

### Main Components

```
GpuParquetScan (DataSourceV2 integration)
    ├── GpuParquetPartitionReaderFactory (per-file or multi-file)
    │   └── ParquetPartitionReader (orchestrates reading)
    │       ├── Footer parsing → GpuParquetFileFilterHandler
    │       ├── Schema clipping → ParquetSchemaUtils
    │       ├── File fabrication → readPartFile()
    │       │   ├── copyBlocksData() or
    │       │   └── copyAndUncompressBlocksData() [CPU decompress]
    │       ├── GPU decode → ParquetChunkedReader (cuDF JNI)
    │       └── Post-process → Schema evolution
    └── GpuReadParquetFileFormat (DataSourceV1 integration)
```

---

## The Fabricated Mini-Parquet Innovation

### What is File Fabrication?

Instead of reading the original Parquet file directly, Spark Rapids **constructs a new, minimal Parquet file** in host memory that contains:
- **Only requested columns** (column pruning applied)
- **Only needed row groups** (partition pruning applied)
- **Valid Parquet format** with updated metadata
- **Optionally decompressed data** (CPU decompression path)

This fabricated file is then sent to the GPU for decoding.

### Why Fabricate Files?

**Problem**: Original Parquet files contain much more data than needed:
- 100 columns when query needs 3
- 1000 row groups when filters match 10
- Data scattered across the file (column chunks interspersed)

**Solution**: Build a compact, sequential file with only what's needed:
- **Minimize GPU memory transfer**: 5MB instead of 1GB
- **Sequential layout**: Column chunks are consecutive
- **Accurate metadata**: Footer describes exactly what's in the buffer
- **GPU efficiency**: cuDF processes clean, optimized input

### Fabrication Process

**Location**: `GpuParquetScan.scala:readPartFile()` (lines 2118-2157)

#### Step 1: Allocate Host Memory

```scala
// Line 2132-2135
val estTotalSize = calculateParquetOutputSize(blocks, clippedSchema)
val outHostBuf = HostMemoryBuffer.allocate(estTotalSize)
val out = new HostMemoryOutputStream(hmb)
```

Pre-calculates exact size needed:
- Header: 4 bytes
- Column chunks: Sum of selected column sizes
- Footer: Based on schema and block metadata
- Trailer: 8 bytes

#### Step 2: Write Parquet Header

```scala
// Line 2138
out.write(ParquetPartitionReader.PARQUET_MAGIC)  // "PAR1"
```

Every valid Parquet file starts with the 4-byte magic: `PAR1`

#### Step 3: Copy Column Chunks

```scala
// Lines 2139-2143
val outputBlocks = if (compressCfg.decompressAnyCpu) {
  copyAndUncompressBlocksData(filePath, out, blocks, out.getPos, metrics, compressCfg)
} else {
  copyBlocksData(filePath, out, blocks, out.getPos, metrics)
}
```

**Without CPU Decompression** (`copyBlocksData`, lines 1735-1770):
```scala
protected def copyBlocksData(
    filePath: Path,
    out: HostMemoryOutputStream,
    blocks: Seq[BlockMetaData],     // Only needed row groups
    realStartOffset: Long,
    metrics: Map[String, GpuMetric]): Seq[BlockMetaData] = {
  
  val remoteItems = new ArrayBuffer[CopyRange]()
  val localItems = new ArrayBuffer[LocalCopy]()
  
  // For each row group, for each column
  blocks.foreach { block =>
    block.getColumns.asScala.foreach { column =>  // Only requested columns!
      val columnSize = column.getTotalSize
      val outputOffset = totalBytesToCopy + startPos
      
      // Check file cache
      val channel = FileCache.get.getDataRangeChannel(
        inputFile, column.getStartingPos, columnSize)
      
      if (channel.isDefined) {
        // Zero-copy from local cache
        localItems += LocalCopy(channel.get, columnSize, outputOffset)
      } else {
        // Read from remote storage
        remoteItems += CopyRange(column.getStartingPos, columnSize, outputOffset)
      }
      totalBytesToCopy += columnSize
    }
  }
  
  // Copy cached data (fast)
  localItems.foreach { item => copyLocal(item, out, metrics) }
  
  // Fetch remote data (batched for efficiency)
  copyRemoteBlocksData(remoteItems.toSeq, filePath, inputFile, out, metrics)
  
  // Return updated metadata
  computeBlockMetaData(blocks, realStartOffset)
}
```

**Key Points**:
- Iterates only through needed row groups (`blocks`)
- For each row group, copies only columns in `clippedSchema`
- Leverages file cache for local data (zero-copy via channel)
- Batches remote reads for efficiency
- Copies compressed data as-is (GPU will decompress)

#### Step 4: Recompute Block Metadata

**Location**: `computeBlockMetaData()` (lines 1685-1721)

```scala
protected def computeBlockMetaData(
    blocks: Seq[BlockMetaData],
    realStartOffset: Long): Seq[BlockMetaData] = {
  
  var totalBytesToCopy = 0L
  val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
  
  blocks.foreach { block =>
    val columns = block.getColumns.asScala
    val outputColumns = new ArrayBuffer[ColumnChunkMetaData](columns.length)
    
    columns.foreach { column =>
      // Calculate offset adjustment
      val startPosCol = column.getStartingPos  // Old offset in original file
      val offsetAdjustment = realStartOffset + totalBytesToCopy - startPosCol
      
      // Update dictionary page offset
      val newDictOffset = if (column.getDictionaryPageOffset > 0) {
        column.getDictionaryPageOffset + offsetAdjustment
      } else {
        0
      }
      
      // Create new metadata with adjusted offsets
      outputColumns += ColumnChunkMetaData.get(
        column.getPath,
        column.getPrimitiveType,
        column.getCodec,
        column.getEncodingStats,
        column.getEncodings,
        column.getStatistics,
        column.getStartingPos + offsetAdjustment,  // NEW OFFSET
        newDictOffset,                              // NEW DICT OFFSET
        column.getValueCount,
        columnSize,
        column.getTotalUncompressedSize)
      
      totalBytesToCopy += columnSize
    }
    outputBlocks += GpuParquetUtils.newBlockMeta(block, outputColumns.toSeq)
  }
  outputBlocks.toSeq
}
```

**Critical**: All file offsets must be recalculated because column chunks have moved to new positions in the fabricated file.

#### Step 5: Write Footer

```scala
// Lines 2144-2145
val footerPos = out.getPos
writeFooter(out, outputBlocks, clippedSchema)
```

**Location**: `writeFooter()` (lines 1633-1643)

```scala
protected def writeFooter(
    out: OutputStream,
    blocks: Seq[BlockMetaData],  // Updated metadata from Step 4
    schema: MessageType): Unit = {
  
  // Create file metadata with clipped schema
  val fileMeta = new FileMetaData(
    schema,  // Only requested columns
    Collections.emptyMap[String, String],
    "RAPIDS Spark Plugin")
  
  val footer = new ParquetMetadata(fileMeta, blocks.asJava)
  val metadataConverter = new ParquetMetadataConverter
  val meta = metadataConverter.toParquetMetadata(1, footer)
  
  // Write Parquet Thrift footer
  org.apache.parquet.format.Util.writeFileMetaData(meta, out)
}
```

Footer contains:
- Clipped schema (only needed columns)
- Updated block metadata (new offsets)
- Row group statistics (preserved from original)
- Creator: "RAPIDS Spark Plugin"

#### Step 6: Write Footer Length and Trailer

```scala
// Lines 2146-2147
BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
out.write(ParquetPartitionReader.PARQUET_MAGIC)  // "PAR1" again
```

Standard Parquet format: last 8 bytes are `[4-byte footer length][PAR1]`

### Visual Example

**Original File (1GB):**
```
┌──────┬────────────────────────────────────────────────┬──────────┬──────┐
│ PAR1 │         Column Chunks (all 100 columns)        │  Footer  │ PAR1 │
│      │  RG0: c0...c99  RG1: c0...c99  RG2: c0...c99  │ (100 col)│      │
│      │  ... 1000 row groups total ...                 │ 1000 RGs │      │
└──────┴────────────────────────────────────────────────┴──────────┴──────┘
  4B                      ~1GB of data                      ~100KB    4B
```

**Query**: `SELECT c3, c7, c9 FROM table WHERE partition = 'X'`
- Partition filter matches row groups: 5, 7, 12
- Column pruning: need c3, c7, c9 only

**Fabricated File (5MB):**
```
┌──────┬─────────────────────────────┬──────────────┬──────┐
│ PAR1 │  Only Needed Column Chunks  │ New Footer   │ PAR1 │
│      │ RG5:  c3, c7, c9           │ (3 columns,  │      │
│      │ RG7:  c3, c7, c9           │  3 row grps) │      │
│      │ RG12: c3, c7, c9           │              │      │
└──────┴─────────────────────────────┴──────────────┴──────┘
  4B              ~5MB                    ~1KB         4B
```

**Metadata Transformation Example:**

Original RG5, Column c3:
```
startingPos: 45,982,736  (45MB into original file)
dictPageOffset: 45,982,800
totalSize: 1,048,576 (1MB)
codec: SNAPPY
```

Fabricated file, RG5, Column c3:
```
startingPos: 4  (right after PAR1 header!)
dictPageOffset: 68
totalSize: 1,048,576 (unchanged)
codec: SNAPPY (unchanged unless CPU decompression enabled)
```

---

## Reading Pipeline

### Two Reading Modes

#### 1. Chunked Reader (Default for Large Files)

**Used when**: File size or estimated output size exceeds thresholds

**Location**: `GpuParquetScan.scala:3377-3381`

```scala
if (useChunkedReader) {
  ParquetTableReader(
    new JniParquetChunkedReader(
      chunkSizeByteLimit,
      maxChunkedReaderMemoryUsageSizeBytes,
      opts, 
      buffers:_*))
}
```

**Characteristics**:
- Streams data in chunks to manage GPU memory
- Calls `reader.next()` to get one chunk at a time
- Configurable chunk size limits
- Better for memory-constrained environments

#### 2. Direct Read (For Smaller Batches)

**Used when**: Data fits comfortably in one batch

**Location**: `GpuParquetScan.scala:3383-3401`

```scala
Table.readParquet(opts, buffers:_*)
```

**Characteristics**:
- Single-shot GPU decode
- Lower overhead (no chunking)
- Faster for small to medium data
- Requires enough GPU memory for entire batch

### GPU Decoding

**Location**: `GpuParquetScan.scala:3467-3483`

```scala
val table = NvtxIdWithMetrics(NvtxRegistry.PARQUET_DECODE, metrics(GPU_DECODE_TIME)) {
  try {
    reader.next  // Calls JniParquetChunkedReader.readChunk() via CUDA
  } catch {
    case e: Exception =>
      // Error handling
  }
}
```

**What happens**:
1. cuDF receives the fabricated Parquet file from host memory
2. GPU kernel parses Parquet pages in parallel
3. If compressed: GPU decompresses using native kernels
4. Outputs GPU-resident `Table` objects (columnar format)

### Post-Processing

**Location**: `GpuParquetScan.scala:3485-3498`

```scala
// DateTime rebasing
val rebasedTable = if (needsDateTimeRebase) {
  rebaseDateTimeColumns(table)
} else {
  table
}

// Schema evolution
val evolvedTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(
  rebasedTable, 
  expectedSchema, 
  isCorrectedRebaseMode)
```

**Operations**:
- **DateTime rebasing**: Convert between Julian and Gregorian calendars if needed
- **Schema evolution**: Handle type mismatches (e.g., int → long, unsigned → signed)
- **Type conversions**: Vectorized GPU casts for efficient transformation

---

## Compression Codec Support

### GPU-Native Decompression (Default)

| Codec | Read Support | Write Support | Availability | Performance |
|-------|--------------|---------------|--------------|-------------|
| **UNCOMPRESSED** | ✅ Yes | ✅ Yes | All versions | N/A |
| **SNAPPY** | ✅ Yes | ✅ Yes | All versions | Very fast |
| **GZIP** | ✅ Yes | ❌ No | All versions | Slower, high compression |
| **ZSTD** | ✅ Yes | ✅ Yes | Spark 3.2.0+ | Best ratio, good speed |

**Architecture**:
```
Disk (compressed) → Host Memory (compressed) → GPU (compressed) → GPU Decompress → GPU (uncompressed)
```

**When to use**: Default for most workloads (GPU-bound, low memory pressure)

### CPU Decompression (Optional)

**Supported codecs**: SNAPPY and ZSTD only

**Configuration**:
```scala
spark.rapids.sql.format.parquet.decompressCpu = true  // Master switch (default: false)
spark.rapids.sql.format.parquet.decompressCpu.snappy = true  // Default: true
spark.rapids.sql.format.parquet.decompressCpu.zstd = true    // Default: true
```

**Architecture**:
```
Disk (compressed) → Host (compressed) → CPU Decompress → Host (uncompressed) → GPU (uncompressed)
```

**When to use**: 
- High GPU memory pressure
- Very high compression ratios (>5:1)
- I/O-bound workloads
- Spare CPU capacity available

**Implementation**: `GpuParquetScan.scala:1905-2010`

### Unsupported Codecs

**Not supported**: LZ4, LZO, BROTLI

**Behavior**: Query errors out (no automatic CPU fallback)

**Workaround**: Recompress files with supported codec

### Detailed Codec Information

For comprehensive compression information, see the "Compression" section in this document or dedicated codec documentation.

---

## Configuration Reference

### Reading Behavior

```scala
// Enable Parquet reading on GPU (master switch)
spark.rapids.sql.format.parquet.enabled
  Type: Boolean
  Default: true
  
// Per-file vs multi-file reading
spark.rapids.sql.format.parquet.read.perFileRead.enabled
  Type: Boolean
  Default: false
  Description: true = one reader per file, false = coalesce multiple files

// Chunked reader control
spark.rapids.sql.format.parquet.chunked.enabled
  Type: Boolean
  Default: true
  Description: Use chunked reader for memory efficiency

// Chunked reader memory limit
spark.rapids.sql.format.parquet.chunkedReader.memoryLimitRatio
  Type: Double
  Default: 0.7
  Description: Fraction of GPU memory available for chunked reading
```

### Batch Sizing

```scala
// Maximum rows per batch
spark.rapids.sql.batchSizeRows
  Type: Integer
  Default: 10000
  Description: Target rows per GPU batch

// Maximum bytes per batch
spark.rapids.sql.batchSizeBytes
  Type: Long
  Default: 2147483647 (2GB - 1)
  Description: Target size per GPU batch
```

### CPU Decompression

```scala
// CPU decompression (see Compression section for details)
spark.rapids.sql.format.parquet.decompressCpu
  Type: Boolean
  Default: false

spark.rapids.sql.format.parquet.decompressCpu.snappy
  Type: Boolean
  Default: true (when master enabled)

spark.rapids.sql.format.parquet.decompressCpu.zstd
  Type: Boolean
  Default: true (when master enabled)
```

### Footer Parsing

```scala
// Footer parsing mode
spark.rapids.sql.parquet.reader.type
  Type: String
  Default: "AUTO"
  Values: "AUTO", "NATIVE", "JAVA"
  Description: 
    - NATIVE: Use native C++ footer parser (fastest)
    - JAVA: Use Java-based parser (compatibility)
    - AUTO: Choose based on schema complexity
```

### Debug and Testing

```scala
// Dump fabricated Parquet files for debugging
spark.rapids.sql.format.parquet.debug.dumpPrefix
  Type: String
  Default: null
  Description: If set, dumps fabricated Parquet files to this path
```

---

## Performance Tuning

### Memory Optimization

#### High GPU Memory Pressure

**Symptoms**: 
- OOM errors during Parquet reads
- GPU memory spilling
- Query failures

**Solutions**:

1. **Enable CPU decompression** (if using compressed files):
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.decompressCpu", "true")
   ```

2. **Reduce batch size**:
   ```python
   spark.conf.set("spark.rapids.sql.batchSizeRows", "5000")
   spark.conf.set("spark.rapids.sql.batchSizeBytes", "1073741824")  # 1GB
   ```

3. **Enable chunked reader** (if disabled):
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.chunked.enabled", "true")
   ```

4. **Adjust chunked reader memory limit**:
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.chunkedReader.memoryLimitRatio", "0.5")
   ```

#### Low GPU Memory Utilization

**Symptoms**:
- GPU memory underutilized
- Small batch sizes

**Solutions**:

1. **Increase batch size**:
   ```python
   spark.conf.set("spark.rapids.sql.batchSizeRows", "20000")
   spark.conf.set("spark.rapids.sql.batchSizeBytes", "4294967296")  # 4GB
   ```

2. **Enable multi-file reading** (coalescing):
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.read.perFileRead.enabled", "false")
   ```

### I/O Optimization

#### Many Small Files

**Problem**: Overhead per file adds up

**Solutions**:

1. **Enable multi-file coalescing**:
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.read.perFileRead.enabled", "false")
   ```

2. **Use multithreaded reader** (if available):
   ```python
   spark.conf.set("spark.rapids.sql.reader.multithreaded.enabled", "true")
   spark.conf.set("spark.rapids.sql.reader.multithreaded.combine.sizeBytes", "67108864")  # 64MB
   ```

#### Slow Remote Storage

**Problem**: High latency to cloud storage (S3, GCS, ABFS)

**Solutions**:

1. **Enable file caching** (if supported):
   - File cache automatically used for repeated reads

2. **Use larger batches** to amortize I/O:
   ```python
   spark.conf.set("spark.rapids.sql.batchSizeBytes", "4294967296")  # 4GB
   ```

3. **Consider compressing output** to reduce I/O volume:
   ```python
   df.write.option("compression", "zstd").parquet("output")
   ```

### Metadata Optimization

#### Complex Schemas

**Problem**: Footer parsing overhead for deeply nested schemas

**Solution**:

1. **Use native footer parser** (default AUTO usually chooses this):
   ```python
   spark.conf.set("spark.rapids.sql.parquet.reader.type", "NATIVE")
   ```

#### Partition Pruning

**Tip**: Ensure partition columns are properly utilized:
- Use partition filters in WHERE clauses
- Partition by frequently filtered columns
- Check query plans with `.explain()` to verify pruning

---

## Code Locations

### Main Implementation

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| **DataSourceV2 Integration** | `GpuParquetScan.scala` | 65-3700 | Main GPU Parquet scan implementation |
| **DataSourceV1 Integration** | `GpuReadParquetFileFormat.scala` | 44-89 | Legacy DataSource API support |
| **Partition Reader Base** | `GpuParquetScan.scala` | 1419-1721 | Base reader functionality |
| **Partition Reader** | `GpuParquetScan.scala` | 3643-3694 | Concrete reader implementation |

### File Fabrication

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| **Main fabrication logic** | `GpuParquetScan.scala` | 2118-2157 | `readPartFile()` |
| **Copy blocks** | `GpuParquetScan.scala` | 1735-1770 | `copyBlocksData()` |
| **Compute metadata** | `GpuParquetScan.scala` | 1685-1721 | `computeBlockMetaData()` |
| **Write footer** | `GpuParquetScan.scala` | 1633-1643 | `writeFooter()` |
| **CPU decompression** | `GpuParquetScan.scala` | 1905-2010 | `copyAndUncompressBlocksData()` |

### Metadata & Schema

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| **Footer handling** | `GpuParquetScan.scala` | 503-699 | Footer parsing and filtering |
| **Schema clipping** | `ParquetSchemaUtils.scala` | 45-60 | `clipParquetSchema()` |
| **Schema evolution** | `ParquetSchemaUtils.scala` | 100-200 | Type conversion logic |

### GPU Decoding

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| **Reader factory** | `GpuParquetScan.scala` | 3351-3418 | `MakeParquetTableProducer` |
| **Chunked reader** | `GpuParquetScan.scala` | 3432-3504 | `ParquetChunkedReader` wrapper |
| **Decode invocation** | `GpuParquetScan.scala` | 3467-3483 | GPU decode call |

### Configuration

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| **Config definitions** | `RapidsConf.scala` | 1400-1450 | Parquet-related configs |
| **Config access** | `RapidsConf.scala` | 3580-3610 | Config accessors |

### Tests

| Component | File | Description |
|-----------|------|-------------|
| **Compression tests** | `integration_tests/src/main/python/parquet_test.py` | Lines 467-492 |
| **General Parquet tests** | `integration_tests/src/main/python/parquet_test.py` | Various |

---

## Integration with Spark

### Query Plan Replacement

Spark Rapids uses Spark's plugin mechanism to replace CPU operators with GPU operators.

**Process**:
1. Spark builds logical plan
2. Spark optimizer creates physical plan
3. **Rapids plugin intercepts** physical plan
4. CPU operators replaced with GPU equivalents
5. GPU plan executed

**For Parquet**:
- `FileSourceScanExec` → `GpuFileSourceScanExec`
- `ParquetScan` → `GpuParquetScan`
- `ParquetFileFormat` → `GpuReadParquetFileFormat`

### DataSource API Integration

#### DataSourceV1 (Legacy)

**Entry Point**: `GpuReadParquetFileFormat.scala:65-89`

```scala
override def buildReaderWithPartitionValues(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
  
  // Returns GPU partition reader
  GpuParquetPartitionReaderFactory(...)
}
```

#### DataSourceV2 (Modern)

**Entry Point**: `GpuParquetScan.scala:125-141`

```scala
override def createReaderFactory(): PartitionReaderFactory = {
  if (rapidsConf.isParquetPerFileReadEnabled) {
    // One reader per file
    GpuParquetPartitionReaderFactory(...)
  } else {
    // Coalesce multiple files
    GpuParquetMultiFilePartitionReaderFactory(...)
  }
}
```

### Partition Reader Lifecycle

```
Spark Driver
    ↓
Creates InputPartitions (one per file or coalesced group)
    ↓
Sends to Executors
    ↓
Executor calls PartitionReaderFactory.createReader()
    ↓
Returns PartitionReader (e.g., ParquetPartitionReader)
    ↓
Spark calls reader.next() repeatedly
    ↓
Reader returns ColumnarBatch (GPU-resident)
    ↓
Downstream GPU operators process batches
```

### ColumnarBatch Format

**GPU Rapids Output**: `ColumnarBatch` containing GPU column vectors

```scala
class ColumnarBatch(
  columns: Array[ColumnVector],  // GPU column vectors
  numRows: Int
)
```

**Each ColumnVector**:
- Backed by GPU memory (via cuDF)
- Supports nested types (arrays, structs, maps)
- Zero-copy between GPU operators
- Spills to host memory if GPU memory exhausted

---

## Troubleshooting

### Common Issues

#### Error: "compression codec [codec] is not supported"

**Cause**: File uses unsupported codec (LZ4, LZO, BROTLI)

**Solutions**:
1. Recompress files with SNAPPY or ZSTD
2. Disable Rapids plugin for this query:
   ```python
   spark.conf.set("spark.rapids.sql.enabled", "false")
   df = spark.read.parquet("unsupported_codec.parquet")
   spark.conf.set("spark.rapids.sql.enabled", "true")
   ```

#### Error: "GPU OutOfMemory" during Parquet read

**Cause**: Fabricated file or decompressed data too large for GPU memory

**Solutions**:
1. Enable CPU decompression (for compressed files):
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.decompressCpu", "true")
   ```

2. Reduce batch size:
   ```python
   spark.conf.set("spark.rapids.sql.batchSizeRows", "5000")
   spark.conf.set("spark.rapids.sql.batchSizeBytes", "536870912")  # 512MB
   ```

3. Enable chunked reader:
   ```python
   spark.conf.set("spark.rapids.sql.format.parquet.chunked.enabled", "true")
   ```

#### Slow performance with small files

**Cause**: Per-file overhead dominates

**Solution**: Enable multi-file coalescing:
```python
spark.conf.set("spark.rapids.sql.format.parquet.read.perFileRead.enabled", "false")
```

#### Schema evolution errors

**Symptom**: Type mismatch errors during Parquet read

**Cause**: Schema changed between write and read

**Solution**: Ensure schema evolution is enabled (default):
```python
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
```

#### Partition filter not applied

**Symptom**: All row groups read despite partition filter

**Diagnosis**: Check query plan:
```python
df.filter("partition_col = 'X'").explain(True)
# Look for "PushedFilters" in scan operator
```

**Solution**: Ensure partition column is properly defined and filter is pushable

---

## Advanced Topics

### File Caching

**Purpose**: Avoid repeated reads of the same data ranges

**Mechanism**:
- `FileCache.get.getDataRangeChannel()` checks cache
- If hit: Zero-copy via memory-mapped channel
- If miss: Read from remote storage and optionally cache

**Configuration**: Automatic, no user configuration needed

**Location**: `GpuParquetScan.scala:1750-1757`

### Multi-File Reading

**Per-File Mode** (`perFileRead.enabled = true`):
- One partition per file
- Good for large files
- Lower overhead for file-level parallelism

**Multi-File Mode** (`perFileRead.enabled = false`, default):
- Coalesces multiple small files
- Better GPU utilization
- Reduces kernel launch overhead

**Selection Logic**: Based on file sizes and partition count

### Native Footer Parsing

**Why Native Parser**:
- Complex schemas (deeply nested) slow in Java
- C++ parser in cuDF is significantly faster
- Directly produces cuDF metadata structures

**Configuration**:
```python
spark.conf.set("spark.rapids.sql.parquet.reader.type", "NATIVE")
```

**Fallback**: AUTO mode selects JAVA parser for certain schema types

### DateTime Rebasing

**Purpose**: Convert between Julian and Gregorian calendars

**When needed**:
- Reading old Parquet files (before Spark 3.0)
- Legacy timestamp formats

**Modes**:
- LEGACY: Rebase everything
- CORRECTED: Rebase only when metadata indicates legacy format
- EXCEPTION: Fail on legacy format

**Configuration**:
```python
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
```

---

## Performance Characteristics

### Speedup Examples

**Typical Speedups** (vs CPU Spark):
- **Large files, simple queries**: 3-10x
- **Complex analytics**: 10-50x
- **Small files**: 1-3x (overhead-limited)

**Factors Affecting Performance**:
1. **Compression ratio**: Higher ratio → more GPU advantage
2. **Column selectivity**: More pruning → better fabrication benefit
3. **Row group selectivity**: More pruning → less I/O
4. **GPU memory**: More memory → larger batches → better throughput
5. **Schema complexity**: Deeply nested → more GPU benefit

### Memory Usage

**Host Memory**:
- Fabricated file: Size of needed columns + row groups
- Typically 10-100MB per partition

**GPU Memory**:
- Compressed data (if GPU decompression)
- Uncompressed columnar data
- Intermediate processing buffers
- Typically 100MB-2GB per batch

### I/O Patterns

**Benefits of Fabrication**:
- **Reduced I/O**: Only read needed bytes
- **Sequential access**: Better cache performance
- **Batched reads**: Coalesced remote reads

**Original file**: 1GB, 100 columns, 1000 row groups
**Query needs**: 3 columns, 10 row groups
**I/O reduction**: ~300x (read 3MB instead of 1GB)

---

## Future Enhancements

### Potential Improvements

1. **Automatic CPU fallback** for unsupported codecs
2. **GPU-side schema evolution** (currently post-processing)
3. **Native encryption support**
4. **Page-level filtering** (skip pages within row groups)
5. **Dictionary-based filtering** (on GPU)

### Experimental Features

Check Rapids documentation for:
- Multithreaded reading
- Advanced caching strategies
- Custom memory allocators

---

## Summary

### Key Takeaways

1. **Fabricated Mini-Parquet**: Core innovation that minimizes GPU data transfer
2. **GPU Decompression**: Default for SNAPPY, GZIP, ZSTD; massively parallel
3. **CPU Decompression**: Optional for SNAPPY/ZSTD; useful for memory pressure
4. **Metadata Filtering**: Native C++ parser for performance
5. **Columnar Processing**: End-to-end GPU pipeline, no row materialization
6. **Configuration**: Extensive tuning options for various workloads

### Decision Trees

**Should I enable CPU decompression?**
```
GPU memory constrained?
├─ YES → Enable CPU decompression
└─ NO → Use default GPU decompression

Compression ratio > 5:1?
├─ YES → Consider CPU decompression
└─ NO → Use GPU decompression

Workload I/O bound?
├─ YES → Consider CPU decompression
└─ NO → Use GPU decompression
```

**Should I enable per-file reading?**
```
Files mostly > 128MB?
├─ YES → Enable per-file (perFileRead.enabled = true)
└─ NO → Use multi-file (default)

Many small files?
├─ YES → Use multi-file (perFileRead.enabled = false)
└─ NO → Use per-file
```

---

## Related Documentation

- **Spark Rapids Compatibility**: `docs/compatibility.md`
- **Configuration Guide**: `docs/additional-functionality/advanced_configs.md`
- **GitHub Issues**: https://github.com/NVIDIA/spark-rapids/issues
- **cuDF Documentation**: https://docs.rapids.ai/api/cudf/stable/

---

## Glossary

- **Fabricated file**: Optimized Parquet file constructed in host memory
- **Row group**: Horizontal partition of a Parquet file (typically 128MB)
- **Column chunk**: Data for one column in one row group
- **Page**: Smallest unit within a column chunk (typically 1MB)
- **cuDF**: RAPIDS GPU DataFrame library (underlying GPU engine)
- **JNI**: Java Native Interface (bridge to cuDF C++ code)
- **Host memory**: CPU-accessible RAM
- **GPU memory**: GPU-accessible VRAM
- **Chunked reader**: Reads data in chunks for memory efficiency
- **Schema evolution**: Handling type changes between write and read schemas
- **DateTime rebasing**: Converting between Julian and Gregorian calendars

---

**Document Version**: 1.0  
**Codebase Version**: Based on commit `21358f330` (main branch, 2026-04-10)  
**Maintained by**: Spark Rapids Community
