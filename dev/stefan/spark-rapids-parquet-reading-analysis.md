# Spark Rapids Parquet Reading Analysis

## Introduction

Code is available at https://github.com/NVIDIA/spark-rapids
and may also be checked out at ../spark-rapids
Agents should prefer local checkout, if available.

## How Spark Rapids Reads Parquet Files

### Row Group-Based Reading

Spark Rapids reads Parquet files **by row groups**, not as whole files.

**Key Evidence:**
- Uses `BlockMetaData` throughout the codebase, which represents Parquet row groups
- `GpuParquetScan.scala:776`: Calls `parquetReader.getRowGroups` to retrieve individual row groups
- `GpuParquetScan.scala:2062-2078`: Iterates over row groups, processing them individually

### Batch Coalescing

Row groups are coalesced into batches based on configurable limits:
- `maxReadBatchSizeRows`: Maximum rows per batch
- `maxReadBatchSizeBytes`: Maximum bytes per batch

The reader:
1. Iterates through row groups (`peekedRowGroup`)
2. Accumulates row groups until size limits are reached
3. Estimates GPU memory per row group before adding to batch

**Code Reference:** `GpuParquetScan.scala:2060-2083`

## Splitting Capability

**YES** - Parquet files can be split into independent processing units.

### Split Boundaries
- **Primary unit**: Row group (Parquet's natural split boundary)
- Row groups are self-contained with their own column chunks and statistics
- Each row group can be processed independently

### Parallel Processing Support

The `MultiFileCloudPartitionReaderBase` architecture supports:
1. **Independent row group processing**: Each row group is a separate work unit
2. **Multi-threaded reading**: Thread pool processes multiple files/row groups in parallel
3. **Memory-bounded execution**: `ResourceBoundedThreadExecutor` manages parallel reads with memory limits

**Code Reference:** `GpuMultiFileReader.scala:496-508`

### Practical Implications

For large Parquet files:
- Files are automatically split at row group boundaries
- Multiple row groups can be processed in parallel (across threads)
- Efficient for distributed processing where different executors handle different row groups
- Row group size at write time affects parallelism potential

## Summary

Spark Rapids processes Parquet files at the **row group granularity**, making row groups the natural split unit for parallel and distributed processing.
