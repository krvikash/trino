# Implementation Plan: GPU-Accelerated Parquet Reading in Trino Hive Connector

## Context

**Why**: Enable GPU-accelerated Parquet file reading in Trino's Hive connector to dramatically improve query performance for large Parquet datasets, especially when reading from remote storage (S3, etc.).

**Problem**: Reading large Parquet files over network is expensive. Without column and row group pruning, we'd transfer massive amounts of unneeded data.

**Solution**: Implement `HivePageSourceProvider.createGpuPageSource()` using RAPIDS cuDF library with file fabrication - creating optimized mini-Parquet files in host memory containing only needed columns and row groups before GPU decode.

**Reference**: Spark Rapids implementation (see `dev/stefan/spark-rapids-parquet-reading-analysis-extended.md`) demonstrates this pattern successfully.

## Key Requirements Summary

Based on interactive discussion with user:

1. **File Fabrication**: Essential - must implement column and row group pruning
2. **Row Group Filtering**: 
   - Split-based: Filter by `HiveSplit.start/length` 
   - Predicate-based: Use `PredicateUtils.getFilteredRowGroups()` for statistics-based pruning
3. **cuDF API**: Single-shot `Table.readParquet()` (not chunked reader for minimal implementation)
4. **File I/O**: All through `TrinoFileSystem` (never Hadoop APIs)
5. **Compression**: GPU decompression only (SNAPPY, GZIP, ZSTD supported)
6. **Column Types**:
   - REGULAR: Read from GPU → `Column.DeviceMemory`
   - PREFILLED: Partition keys → `Column.Blocks` with `RunLengthEncodedBlock`
   - INTERIM, SYNTHESIZED, EMPTY: Fail explicitly with `UnsupportedOperationException`
7. **Type Support**: Only existing types in `GpuTypeConversion`: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, VARCHAR
8. **Column Matching**: Case-insensitive name matching, separated into interface for future Iceberg field-ID support
9. **Error Handling**: Fail explicitly, no CPU fallback

## Architecture

### Component Structure

```
HivePageSourceProvider.createGpuPageSource()
    ↓
GpuParquetPageSource (implements ConnectorGpuPageSource)
    ├── ParquetFileFabricator (fabricates mini-Parquet in host memory)
    │   ├── Footer parsing (MetadataReader.readFooter)
    │   ├── Row group filtering (PredicateUtils.getFilteredRowGroups)
    │   ├── Column chunk copying (via TrinoInputFile streams)
    │   └── Footer writing (Parquet format compliance)
    └── ColumnMatchingStrategy (interface for column resolution)
        └── NameBasedColumnMatcher (case-insensitive, for Hive)
```

### New Classes to Create

#### 1. `GpuParquetPageSource.java`
**Location**: `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/GpuParquetPageSource.java`

**Purpose**: Main GPU page source implementing `ConnectorGpuPageSource`

**Key responsibilities**:
- Orchestrate fabrication → GPU decode pipeline
- Mix GPU columns (REGULAR) with CPU columns (PREFILLED partition keys)
- Manage cuDF `Table` lifecycle with proper reference counting
- State machine: NEEDS_FABRICATION → HAS_BUFFER → FINISHED

**Key methods**:
- `readNext()`: Returns `Result` (Yielded during fabrication, Data with GpuPage after decode, Finished when done)
- `convertToGpuPage(Table)`: Creates `GpuPage` mixing `Column.DeviceMemory` (GPU) and `Column.Blocks` (partition keys)
- `close()`: Cleanup resources

#### 2. `ParquetFileFabricator.java`
**Location**: `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/ParquetFileFabricator.java`

**Purpose**: Creates optimized mini-Parquet files in host memory

**Key responsibilities**:
- Parse Parquet footer via `MetadataReader.readFooter()`
- Filter row groups by split range AND predicates
- Calculate exact buffer size needed
- Copy only needed column chunks from filtered row groups
- Recalculate all file offsets for new layout
- Write valid Parquet footer and trailer

**Key methods**:
- `fabricate()`: Returns `HostMemoryBuffer` with fabricated file
- `filterRowGroups()`: Uses `PredicateUtils.getFilteredRowGroups()`
- `copyColumnChunks()`: Reads chunks via `TrinoInputFile.newStream()`, tracks new offsets
- `writeFabricatedFile()`: Assembles PAR1 header + chunks + footer + size + PAR1 trailer

#### 3. `ColumnMatchingStrategy.java` (Interface)
**Location**: `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/ColumnMatchingStrategy.java`

**Purpose**: Abstract column matching for different formats

**Methods**:
- `Optional<Type> findColumn(HiveColumnHandle, MessageType)`: Find Parquet field for Hive column
- `MessageType clipSchema(MessageType, List<HiveColumnHandle>)`: Build schema with only requested columns

#### 4. `NameBasedColumnMatcher.java`
**Location**: `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/NameBasedColumnMatcher.java`

**Purpose**: Case-insensitive name-based column matching for Hive

**Implementation**: Iterate schema fields, compare names case-insensitively

## Detailed Implementation Steps

### Step 1: Modify `HivePageSourceProvider.createGpuPageSource()`

**File**: `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/HivePageSourceProvider.java:107-125`

**Actions**:
1. Check if split is Parquet format (examine `hiveSplit.getSchema()`)
2. Build `ColumnMapping` objects (reuse from CPU path: `ColumnMapping.buildColumnMappings()`)
3. Validate only REGULAR and PREFILLED columns (fail on INTERIM, SYNTHESIZED, EMPTY)
4. Validate all REGULAR column types are supported by `GpuTypeConversion.isConvertible()`
5. Extract REGULAR columns for GPU reading
6. Create `TrinoInputFile` from `TrinoFileSystem`
7. Create `ParquetFileFabricator` with split range, columns, predicates, matcher
8. Return `Optional.of(new GpuParquetPageSource(...))`

### Step 2: Implement `ParquetFileFabricator`

**File Fabrication Pipeline** (following Spark Rapids pattern):

#### Phase 1: Parse Footer
```java
ParquetDataSource dataSource = new TrinoParquetDataSource(...);
ParquetMetadata metadata = MetadataReader.readFooter(dataSource, options);
MessageType fullSchema = metadata.getFileMetaData().getSchema();
```

#### Phase 2: Filter Row Groups
```java
// Build predicate (reuse existing Trino logic from ParquetPageSourceFactory)
TupleDomain<ColumnDescriptor> parquetPredicate = convertToParquetPredicate(...);
TupleDomainParquetPredicate predicate = buildPredicate(...);

// Filter using PredicateUtils (handles both split range and statistics)
List<RowGroupInfo> filteredRowGroups = PredicateUtils.getFilteredRowGroups(
    splitStart,           // HiveSplit.getStart()
    splitLength,          // HiveSplit.getLength()
    dataSource,
    metadata,
    List.of(parquetPredicate),
    List.of(predicate),
    descriptorsByPath,
    timeZone,
    domainCompactionThreshold,
    options
);
```

#### Phase 3: Clip Schema
```java
MessageType clippedSchema = columnMatcher.clipSchema(fullSchema, gpuColumns);
// Result: Schema contains only requested columns (e.g., c3, c7, c9 instead of c0-c99)
```

#### Phase 4: Calculate Buffer Size
```java
long size = 4; // PAR1 header
for (RowGroupInfo rg : filteredRowGroups) {
    for (ColumnChunkMetadata col : rg.blockMetadata().columns()) {
        if (isColumnInClippedSchema(col, clippedSchema)) {
            size += col.getTotalSize();
        }
    }
}
size += estimateFooterSize(filteredRowGroups, clippedSchema);
size += 4; // footer length (int)
size += 4; // PAR1 trailer
```

#### Phase 5: Allocate and Write Fabricated File
```java
HostMemoryBuffer buffer = HostMemoryBuffer.allocate(size);
try {
    writeFabricatedFile(buffer, filteredRowGroups, clippedSchema, metadata);
    return buffer;
} catch (Exception e) {
    buffer.close();
    throw e;
}
```

#### Phase 6: Write Fabricated File Contents

**Format** (standard Parquet):
```
[PAR1 header: 4 bytes]
[Column chunks: variable size, only needed columns from filtered row groups]
[Footer: Thrift-encoded metadata]
[Footer size: 4 bytes little-endian]
[PAR1 trailer: 4 bytes]
```

**Implementation**:
```java
void writeFabricatedFile(HostMemoryBuffer buffer, ...) {
    HostMemoryOutputStream out = new HostMemoryOutputStream(buffer);
    
    // 1. Write PAR1 header
    out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
    long dataStartOffset = 4;
    
    // 2. Copy column chunks
    List<BlockMetadata> updatedBlocks = new ArrayList<>();
    long currentOffset = dataStartOffset;
    
    for (RowGroupInfo rgInfo : filteredRowGroups) {
        BlockMetadata block = rgInfo.blockMetadata();
        List<ColumnChunkMetadata> updatedColumns = new ArrayList<>();
        
        for (ColumnChunkMetadata column : block.columns()) {
            if (!isColumnInClippedSchema(column, clippedSchema)) continue;
            
            // Read chunk from original file
            long chunkOffset = column.getStartingPos();
            long chunkSize = column.getTotalSize();
            try (InputStream in = inputFile.newStream()) {
                in.skip(chunkOffset);
                byte[] chunkData = in.readNBytes((int) chunkSize);
                out.write(chunkData);
            }
            
            // Update metadata with new offset
            long offsetAdjustment = currentOffset - chunkOffset;
            ColumnChunkMetadata updatedColumn = ColumnChunkMetadata.get(
                column.getPath(),
                column.getPrimitiveType(),
                column.getCodec(),
                column.getEncodingStats(),
                column.getEncodings(),
                column.getStatistics(),
                currentOffset,  // NEW starting position
                column.getDictionaryPageOffset() > 0 
                    ? column.getDictionaryPageOffset() + offsetAdjustment 
                    : 0,
                column.getValueCount(),
                column.getTotalSize(),
                column.getTotalUncompressedSize()
            );
            updatedColumns.add(updatedColumn);
            currentOffset += chunkSize;
        }
        
        updatedBlocks.add(new BlockMetadata(
            block.fileRowCountOffset(),
            block.rowCount(),
            updatedColumns
        ));
    }
    
    // 3. Write footer
    long footerStart = out.getPos();
    FileMetadata newFileMetadata = new FileMetadata(
        clippedSchema,
        Collections.emptyMap(),
        "Trino GPU Parquet Reader"
    );
    ParquetMetadata newMetadata = new ParquetMetadata(newFileMetadata, updatedBlocks);
    
    // Serialize using Parquet Thrift format
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    org.apache.parquet.format.FileMetaData thriftMetadata = 
        converter.toParquetMetadata(1, newMetadata);
    org.apache.parquet.format.Util.writeFileMetaData(thriftMetadata, out);
    
    // 4. Write footer size (little-endian)
    int footerSize = (int) (out.getPos() - footerStart);
    out.write(ByteBuffer.allocate(4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(footerSize)
        .array());
    
    // 5. Write PAR1 trailer
    out.write("PAR1".getBytes(StandardCharsets.US_ASCII));
}
```

**Critical**: Offset recalculation is essential - all column chunk positions and dictionary page offsets must be updated to reflect new positions in fabricated file.

### Step 3: Implement `GpuParquetPageSource`

**State Machine**:
```
NEEDS_FABRICATION → fabricate() → HAS_BUFFER → decode() → FINISHED
```

**Key implementation**:

```java
public class GpuParquetPageSource implements ConnectorGpuPageSource {
    private enum State { NEEDS_FABRICATION, HAS_BUFFER, FINISHED }
    
    private State state = State.NEEDS_FABRICATION;
    private HostMemoryBuffer fabricatedBuffer;
    
    @Override
    public Result readNext() {
        return switch (state) {
            case NEEDS_FABRICATION -> {
                fabricatedBuffer = fabricator.fabricate();
                state = State.HAS_BUFFER;
                yield new Yielded(); // Give scheduler a chance
            }
            
            case HAS_BUFFER -> {
                try {
                    GpuPage page = readAndConvert();
                    state = State.FINISHED;
                    yield new Data(page);
                } finally {
                    fabricatedBuffer.close();
                    fabricatedBuffer = null;
                }
            }
            
            case FINISHED -> new Finished();
        };
    }
    
    private GpuPage readAndConvert() {
        // Build cuDF ParquetOptions
        ParquetOptions.Builder optionsBuilder = ParquetOptions.builder();
        for (HiveColumnHandle col : gpuColumns) {
            optionsBuilder.includeColumn(col.getBaseColumnName());
        }
        ParquetOptions options = optionsBuilder.build();
        
        // Read from fabricated buffer
        Table table = Table.readParquet(options, fabricatedBuffer);
        try {
            // Validate column types match expectations
            validateColumnTypes(table);
            
            // Convert to GpuPage
            return convertToGpuPage(table);
        } finally {
            table.close();
        }
    }
    
    private GpuPage convertToGpuPage(Table table) {
        Column[] columns = new Column[columnMappings.size()];
        int gpuColumnIndex = 0;
        int rowCount = (int) table.getRowCount();
        
        for (int i = 0; i < columnMappings.size(); i++) {
            ColumnMapping mapping = columnMappings.get(i);
            
            columns[i] = switch (mapping.getKind()) {
                case REGULAR -> {
                    // GPU column: increment refcount before storing
                    ColumnVector cudfCol = table.getColumn(gpuColumnIndex++);
                    yield new Column.DeviceMemory(cudfCol.incRefCount());
                }
                
                case PREFILLED -> {
                    // Partition key: create RLE block
                    Block rleBlock = RunLengthEncodedBlock.create(
                        mapping.getHiveColumnHandle().getType(),
                        mapping.getPrefilledValue(),
                        rowCount
                    );
                    yield new Column.Blocks(List.of(rleBlock));
                }
                
                case INTERIM, SYNTHESIZED, EMPTY -> 
                    throw new TrinoException(
                        HIVE_UNSUPPORTED_FORMAT,
                        "GPU Parquet reader does not support column type: " + 
                        mapping.getKind()
                    );
            };
        }
        
        return new GpuPage(rowCount, columns);
    }
}
```

**Memory Management**:
- `HostMemoryBuffer`: Closed after GPU read completes
- cuDF `Table`: Closed immediately after conversion
- `ColumnVector`: Reference count incremented before storing in `Column.DeviceMemory`
- `GpuPage` owns columns, will close them when `GpuPage.close()` is called

### Step 4: Implement Column Matching

**Interface** (`ColumnMatchingStrategy.java`):
```java
public interface ColumnMatchingStrategy {
    Optional<Type> findColumn(HiveColumnHandle column, MessageType schema);
    MessageType clipSchema(MessageType fullSchema, List<HiveColumnHandle> columns);
}
```

**Implementation** (`NameBasedColumnMatcher.java`):
```java
public class NameBasedColumnMatcher implements ColumnMatchingStrategy {
    @Override
    public Optional<Type> findColumn(HiveColumnHandle column, MessageType schema) {
        String columnName = column.getBaseColumnName();
        for (Type field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(columnName)) {
                return Optional.of(field);
            }
        }
        return Optional.empty();
    }
    
    @Override
    public MessageType clipSchema(MessageType fullSchema, List<HiveColumnHandle> columns) {
        List<Type> clippedFields = new ArrayList<>();
        for (HiveColumnHandle column : columns) {
            findColumn(column, fullSchema).ifPresent(clippedFields::add);
        }
        return new MessageType(fullSchema.getName(), clippedFields);
    }
}
```

**Future**: Add `FieldIdColumnMatcher` for Iceberg that uses field IDs instead of names.

### Step 5: Error Handling & Validation

**Validation points**:

1. **Type support** (in `createGpuPageSource`):
```java
for (ColumnMapping mapping : columnMappings) {
    if (mapping.getKind() == ColumnMappingKind.REGULAR) {
        if (!GpuTypeConversion.isConvertible(mapping.getHiveColumnHandle().getType())) {
            throw new TrinoException(
                HIVE_UNSUPPORTED_FORMAT,
                "GPU Parquet reader does not support type: " + 
                mapping.getHiveColumnHandle().getType()
            );
        }
    }
}
```

2. **Column type support** (in `createGpuPageSource`):
```java
for (ColumnMapping mapping : columnMappings) {
    if (mapping.getKind() != ColumnMappingKind.REGULAR && 
        mapping.getKind() != ColumnMappingKind.PREFILLED) {
        throw new TrinoException(
            HIVE_UNSUPPORTED_FORMAT,
            "GPU reader does not support column kind: " + mapping.getKind()
        );
    }
}
```

3. **Compression codec** (in `ParquetFileFabricator`):
```java
for (ColumnChunkMetadata column : block.columns()) {
    CompressionCodecName codec = column.getCodec();
    if (codec != UNCOMPRESSED && codec != SNAPPY && 
        codec != GZIP && codec != ZSTD) {
        throw new TrinoException(
            HIVE_UNSUPPORTED_FORMAT,
            "GPU Parquet reader does not support codec: " + codec
        );
    }
}
```

4. **Empty result** (in `ParquetFileFabricator`):
```java
if (filteredRowGroups.isEmpty()) {
    // Create minimal empty Parquet file
    // OR return special marker and handle in GpuParquetPageSource
}
```

## Critical Files to Modify/Create

### Modify
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/HivePageSourceProvider.java`
  - Lines 107-125: Replace stub implementation in `createGpuPageSource()`
  - Add format detection, column mapping validation, GPU page source creation

### Create
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/GpuParquetPageSource.java`
  - Main GPU page source with state machine and cuDF integration
  
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/ParquetFileFabricator.java`
  - File fabrication logic with footer parsing, filtering, chunk copying, offset recalculation
  
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/ColumnMatchingStrategy.java`
  - Interface for column matching abstraction
  
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/NameBasedColumnMatcher.java`
  - Case-insensitive name-based matching implementation

### Reference (do not modify, reuse code patterns)
- `/home/ubuntu/cork/plugin/trino-hive/src/main/java/io/trino/plugin/hive/parquet/ParquetPageSourceFactory.java`
  - Reuse: Footer parsing pattern, predicate building, column descriptor mapping
  
- `/home/ubuntu/cork/lib/trino-parquet/src/main/java/io/trino/parquet/predicate/PredicateUtils.java`
  - Use: `getFilteredRowGroups()` for row group filtering
  
- `/home/ubuntu/cork/lib/trino-parquet/src/main/java/io/trino/parquet/reader/MetadataReader.java`
  - Use: `readFooter()` for parsing Parquet metadata

## Verification Plan

### Unit Testing
1. `TestNameBasedColumnMatcher`: Verify case-insensitive matching
2. `TestParquetFileFabricator`: Test fabrication with mock data
3. `TestGpuParquetPageSource`: Test state machine and conversion

### Integration Testing
1. Create test Parquet file with known schema and data
2. Query via GPU path, verify results match CPU path
3. Test column pruning (request subset of columns)
4. Test row group pruning (partition filters, predicates)
5. Test partition keys (PREFILLED columns)
6. Test error cases (unsupported types, codecs, column kinds)

### End-to-End Testing
1. Create Hive table backed by Parquet on S3
2. Run query requesting GPU execution
3. Verify:
   - Only needed columns read from S3
   - Only matching row groups processed
   - Results correct
   - Performance improvement over CPU path

### Manual Verification
```sql
-- Enable GPU execution
SET SESSION hive.gpu_enabled = true;

-- Query with column pruning
SELECT col1, col3 FROM large_parquet_table WHERE partition_key = 'value';

-- Verify via EXPLAIN
EXPLAIN SELECT col1, col3 FROM large_parquet_table WHERE partition_key = 'value';
-- Should show GPU operators in plan
```

## Performance Considerations

### Memory Usage
- **Host memory**: One fabricated file per split (typically 1-10% of original)
- **GPU memory**: Uncompressed columnar data (released after `GpuPage` produced)
- **Lifetime**: Fabricated buffer released immediately after GPU decode

### I/O Optimization
- Column pruning: Only read needed column chunks (not entire file)
- Row group pruning: Skip row groups filtered by predicates/split range
- Sequential access: Fabricated file has sequential layout
- Reuse existing `TrinoInputFile` caching

### GPU Utilization
- Single-shot decode: Minimal overhead
- GPU decompression: Parallel processing of compressed data
- No CPU decompression overhead in this initial implementation

## Future Enhancements

1. **Iceberg Support**: Implement `FieldIdColumnMatcher` for field-ID-based matching
2. **CPU Decompression**: Add optional CPU decompression during fabrication for memory-constrained cases
3. **Chunked Reading**: Use `JniParquetChunkedReader` for very large row groups
4. **More Types**: Add DATE, TIMESTAMP, DECIMAL support
5. **Schema Evolution**: Handle type conversions on GPU
6. **Additional Column Kinds**: Support SYNTHESIZED (row IDs) if needed

## Dependencies

- cuDF 26.02.0 (already in `pom.xml`)
- Existing Trino Parquet infrastructure (`trino-parquet` module)
- GPU SPI classes (`trino-spi` module)
- `GpuTypeConversion` for type mapping

## Risks & Mitigation

**Risk**: Offset calculation errors in fabricated file
- **Mitigation**: Thorough testing, validate against cuDF's expectations

**Risk**: Memory leaks from improper reference counting
- **Mitigation**: Follow try-with-resources pattern, explicit `incRefCount()` before ownership transfer

**Risk**: Unsupported Parquet features causing failures
- **Mitigation**: Explicit validation and clear error messages, fail fast

**Risk**: Performance regression for small files
- **Mitigation**: Fabrication overhead is minimal for small files, GPU decode is fast

## Success Criteria

1. ✅ GPU reads Parquet files with column pruning
2. ✅ GPU reads Parquet files with row group pruning (split-based + predicate-based)
3. ✅ Partition keys work (PREFILLED columns as RunLengthEncodedBlock)
4. ✅ Results match CPU Parquet reader
5. ✅ Memory managed correctly (no leaks)
6. ✅ Clear error messages for unsupported features
7. ✅ Performance improvement over CPU for large files with pruning
