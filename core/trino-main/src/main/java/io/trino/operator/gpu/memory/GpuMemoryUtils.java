/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.gpu.memory;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.NullEquality;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.ast.CompiledExpression;
import com.google.common.math.LongMath;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.ceilDiv;

public final class GpuMemoryUtils
{
    private GpuMemoryUtils() {}

    // Each slot holds cuco::pair<uint32_t, int32_t> = 8 bytes
    private static final long HASH_TABLE_ENTRY_BYTES = Integer.BYTES + Integer.BYTES;
    // Each slot holds size_type (int32_t) = 4 bytes
    private static final long HASH_SET_ENTRY_BYTES = Integer.BYTES;
    // sizeof(cudf::column_device_view)
    private static final long COLUMN_DEVICE_VIEW_BYTES = 64;
    // cudf::detail::DEFAULT_JOIN_CG_SIZE
    private static final long JOIN_CG_SIZE = 2;
    // cuco::storage<2> used by cudf::hash_join
    private static final long JOIN_BUCKET_SIZE = 2;
    // Measured fixed cost of a mixed join: the cub temporary storage base plus the parsed AST of a
    // small predicate, neither of which is worth modelling in detail
    private static final long MIXED_JOIN_FIXED_BYTES = 1157;
    // Measured fixed cost of Table.dropDuplicates
    private static final long DISTINCT_FIXED_BYTES = 1152;
    // Measured fixed cost of GpuJoinMatchMask#unmatchedBuildIndices: allocation rounding and cub temporaries
    private static final long UNMATCHED_BUILD_INDICES_FIXED_BYTES = 1536;

    /// Estimate the additional GPU device memory retained by `new HashJoin(buildKeys, [false])`,
    /// assuming `buildKeys` is retained by the caller and therefore not counted here.
    // TODO might become obsolete when https://github.com/NVIDIA/cudf/issues/22965 is done
    public static long getHashJoinAdditionalGpuDeviceMemoryUsage(Table buildKeys)
    {
        long rowCount = buildKeys.getRowCount();
        if (rowCount == 0) {
            return 0;
        }
        return joinHashTableBytes(rowCount) + preprocessedTableBytes(buildKeys);
    }

    /// Estimates the temporary GPU device memory consumed during a call to
    /// {@link ColumnVector#contains(ai.rapids.cudf.ColumnView)}, excluding the output boolean column.
    public static long getContainsGpuDeviceMemoryUsage(Table haystack)
    {
        long rowCount = haystack.getRowCount();
        if (rowCount == 0) {
            return 0;
        }
        // cuDF uses linear_probing<CG_SIZE=1> for flat types and linear_probing<CG_SIZE=4> for nested types.
        long cucoBytes = (isNested(haystack) ? cucoHashTableCapacity(rowCount) : cucoHashSetCapacity(rowCount)) * HASH_SET_ENTRY_BYTES;
        // contains() creates two preprocessed_table views (haystack + needles),
        long preprocessedBytes = 2 * preprocessedTableBytes(haystack);
        // bucket_storage over-allocates by (alignment - 1) / sizeof(value_type) + 1 elements for alignment;
        // for int32_t keys with alignof(int32_t) = 4: extra = (4 - 1) / 4 + 1 = 1 element = 4 bytes
        return cucoBytes + 4 + preprocessedBytes;
    }

    /// Peak of copying `rows` INT32 indices and deduplicating them with {@link Table#dropDuplicates}.
    public static long getDistinctIndicesGpuDeviceMemoryUsage(long rows)
    {
        if (rows == 0) {
            return 0;
        }
        // Copy, hash set and distinct indices are live together
        long copyBytes = rows * Integer.BYTES;
        long hashSetBytes = cucoHashSetCapacity(rows) * HASH_SET_ENTRY_BYTES;
        long distinctIndexBytes = rows * Integer.BYTES;
        // Measured cub temporary storage
        long temporaryBytes = DISTINCT_FIXED_BYTES + rows / 64;
        return copyBytes + hashSetBytes + distinctIndexBytes + temporaryBytes;
    }

    /// Peak of {@link io.trino.operator.gpu.join.GpuJoinMatchMask#unmatchedBuildIndices}, assuming every build row is unmatched.
    public static long getUnmatchedBuildIndicesGpuDeviceMemoryUsage(long buildRowCount)
    {
        long sequenceBytes = buildRowCount * Integer.BYTES;
        long invertedMaskBytes = buildRowCount * Byte.BYTES;
        // Filter output plus its gather map, as in getFilterGpuDeviceMemoryUsage
        long filterBytes = 2 * buildRowCount * Integer.BYTES;
        return sequenceBytes + invertedMaskBytes + filterBytes + UNMATCHED_BUILD_INDICES_FIXED_BYTES;
    }

    private static boolean isNested(Table table)
    {
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            DType.DTypeEnum typeId = table.getColumn(i).getType().getTypeId();
            if (typeId == DType.DTypeEnum.STRING || typeId == DType.DTypeEnum.LIST) {
                return true;
            }
        }
        return false;
    }

    private static long preprocessedTableBytes(Table table)
    {
        // preprocessed_table::create allocates a table_device_view whose size depends on the C++
        // column_device_view children count (sizeof(column_device_view)=64 bytes each):
        // STRING has 1 C++ child (offsets), LIST has 2 C++ children (offsets + elements)
        long bytes = 79;
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            bytes += COLUMN_DEVICE_VIEW_BYTES * childColumnCount(table.getColumn(i).getType());
        }
        return bytes;
    }

    /// Bytes of the `cudf::table_device_view` cuDF builds for a table: one `column_device_view` per
    /// column plus one per C++ child column.
    private static long tableDeviceViewBytes(Table table)
    {
        long bytes = 0;
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            bytes += COLUMN_DEVICE_VIEW_BYTES * (1 + childColumnCount(table.getColumn(i).getType()));
        }
        return bytes;
    }

    /// Number of C++ child columns a `cudf::column_device_view` of this type has: STRING has one
    /// (offsets), LIST has two (offsets and elements).
    private static long childColumnCount(DType type)
    {
        DType.DTypeEnum typeId = type.getTypeId();
        if (typeId == DType.DTypeEnum.STRING) {
            return 1;
        }
        if (typeId == DType.DTypeEnum.LIST) {
            return 2;
        }
        return 0;
    }

    /// Estimates the peak temporary GPU device memory consumed by a single call to
    /// {@link Table#mixedInnerJoinGatherMaps(Table, Table, Table, Table, CompiledExpression, NullEquality)}.
    ///
    /// `equalityMatchCount` is the number of index pairs produced by the equality part of the join
    /// alone, as reported by {@link Table#innerJoinRowCount(ai.rapids.cudf.HashJoin)} for a hash join
    /// built over `buildKeys`. Since cuDF 26.08 the mixed join materializes all of those pairs before
    /// evaluating the AST predicate, so the peak is a function of join selectivity and cannot be
    /// derived from row counts alone.
    ///
    /// The estimate excludes the output gather maps whose size depends on how many pairs survive the
    /// predicate. `equalityMatchCount` must be positive: a join with no equality match produces no row
    /// at all, so callers are expected to skip it rather than estimate it.
    public static long getMixedInnerJoinGpuDeviceMemoryUsage(Table buildKeys, Table buildSource, Table probeSource, long equalityMatchCount)
    {
        return estimateMixedJoinBytes(buildKeys, buildSource, probeSource, equalityMatchCount, false);
    }

    /// Estimates the peak temporary GPU device memory consumed by a single call to
    /// {@link Table#mixedLeftJoinGatherMaps(Table, Table, Table, Table, CompiledExpression, NullEquality)}.
    ///
    /// See {@link #getMixedInnerJoinGpuDeviceMemoryUsage} for `equalityMatchCount`, which here is
    /// {@link Table#leftJoinRowCount(ai.rapids.cudf.HashJoin)}.
    public static long getMixedLeftJoinGpuDeviceMemoryUsage(Table buildKeys, Table buildSource, Table probeSource, long equalityMatchCount)
    {
        return estimateMixedJoinBytes(buildKeys, buildSource, probeSource, equalityMatchCount, true);
    }

    /// Models `cudf::detail::mixed_join` as of cuDF 26.08: it builds a `cudf::hash_join` over the
    /// build (right) equality keys, materializes the complete unfiltered equality match pairs with
    /// `equality_join_indices`, then applies the AST predicate and compacts with
    /// `filter_join_indices`. There is no chunking, so everything below is live at the same time.
    private static long estimateMixedJoinBytes(Table buildKeys, Table buildSource, Table probeSource, long equalityMatchCount, boolean trackUnmatchedProbeRows)
    {
        checkArgument(equalityMatchCount > 0, "equalityMatchCount must be positive");
        long buildRows = buildKeys.getRowCount();
        // The hash table over the build equality keys, rebuilt on every call. While it is being built,
        // hash_join also holds a transient validity bitmask over the build keys; that is added rather
        // than maximized against the pairs below, which overestimates by at most 0.8% of the table.
        long hashTableBytes = joinHashTableBytes(buildRows) + preprocessedTableBytes(buildKeys)
                + nullMaskBytes(buildRows);
        // equality_join_indices materializes two int32 gather maps covering every equality match, and
        // filter_join_indices allocates one bool per match to hold the predicate results
        long matchBytes = equalityMatchCount * (2 * Integer.BYTES + 1);
        // cub tile state backing the stream compaction over the match pairs; measured at <= matches/80
        long compactionBytes = equalityMatchCount / 64;
        long unmatchedTrackingBytes = 0;
        if (trackUnmatchedProbeRows) {
            unmatchedTrackingBytes = filterPassingSetBytes(probeSource.getRowCount());
        }
        // table_device_view of both conditional tables, plus the parsed AST and the fixed part of the
        // cub temporary storage. The AST part grows with expression size; it is a few hundred bytes for
        // the expressions cork produces and is dwarfed by matchBytes for any non-trivial join.
        long fixedBytes = tableDeviceViewBytes(buildSource) + tableDeviceViewBytes(probeSource) + MIXED_JOIN_FIXED_BYTES;
        return hashTableBytes + matchBytes + compactionBytes + unmatchedTrackingBytes + fixedBytes;
    }

    /// Bytes held by the `cuco::static_multiset` backing `cudf::hash_join`.
    ///
    /// cuDF builds it with `CUCO_DESIRED_LOAD_FACTOR = 0.5`, `cuco::storage<2>` and
    /// `cuco::double_hashing<DEFAULT_JOIN_CG_SIZE = 2>`, so the requested capacity is `2 * rowCount`
    /// slots, which cuco groups into `ceilDiv(2 * rowCount, cgSize * bucketSize)` groups. Double
    /// hashing requires a prime number of groups, so cuco rounds that up to the next prime, then
    /// over-allocates one bucket for alignment.
    private static long joinHashTableBytes(long rowCount)
    {
        long groups = nextPrime(ceilDiv(2 * rowCount, JOIN_CG_SIZE * JOIN_BUCKET_SIZE));
        return (groups * JOIN_CG_SIZE + 1) * JOIN_BUCKET_SIZE * HASH_TABLE_ENTRY_BYTES;
    }

    /// Bytes held by the `cuco::static_set` that `filter_join_indices` uses on left joins to record
    /// which probe rows kept at least one match. It uses `CUCO_DESIRED_LOAD_FACTOR = 0.5` with the
    /// default `cuco::storage<1>` and `cuco::double_hashing<1>`, so the group count is `2 * probeRows`
    /// rounded up to the next prime, plus one bucket for alignment.
    private static long filterPassingSetBytes(long probeRows)
    {
        return (nextPrime(2 * probeRows) + 1) * HASH_SET_ENTRY_BYTES;
    }

    /// Smallest prime greater than or equal to `value`, matching `cuco::detail::next_prime`.
    private static long nextPrime(long value)
    {
        long candidate = Math.max(value, 2);
        while (!LongMath.isPrime(candidate)) {
            candidate++;
        }
        return candidate;
    }

    private static long cucoHashTableCapacity(long rowCount)
    {
        // cuco::static_multiset with CUCO_DESIRED_LOAD_FACTOR=0.5, storage<2>, cooperative group size=2:
        // rawCapacity = ceil(rowCount / 0.5) = 2 * rowCount
        // capacity = CG_SIZE * ceilDiv(rawCapacity, CG_SIZE * WINDOW_SIZE) * WINDOW_SIZE
        //          = 4 * ceilDiv(rowCount, 2)
        return 4L * ((rowCount + 1) / 2);
    }

    private static long cucoHashSetCapacity(long rowCount)
    {
        // cuco::static_set with CUCO_DESIRED_LOAD_FACTOR=0.5, storage<1>, linear_probing<CG_SIZE=1>:
        // rawCapacity = ceil(rowCount / 0.5) = 2 * rowCount
        // capacity = CG_SIZE * ceilDiv(rawCapacity, CG_SIZE * WINDOW_SIZE) * WINDOW_SIZE
        //          = 1 * ceilDiv(2 * rowCount, 1) * 1 = 2 * rowCount
        return 2L * rowCount;
    }

    /// cuDF validity mask: one bit per row, `ceil(rows / 8)` bytes padded to a 64-byte boundary.
    public static long nullMaskBytes(long rowCount)
    {
        return 64 * ceilDiv(ceilDiv(rowCount, 8), 64);
    }

    public static long getNullColumnMemoryUsage(DType type, int positionCount)
    {
        long nullMaskBytes = nullMaskBytes(positionCount);
        int sizeInBytes = type.getSizeInBytes();
        if (sizeInBytes == 0) {
            // Variable-width: offsets array with N+1 INT32 elements (pairs of consecutive offsets
            // define start/end of each value's chars), empty chars buffer since all values are null
            return 4L * (positionCount + 1) + nullMaskBytes;
        }
        return (long) sizeInBytes * positionCount + nullMaskBytes;
    }

    public static long getFilterGpuDeviceMemoryUsage(@Borrow GpuPage input, int retainedPositionCount)
    {
        long estimatedOutputDeviceBytes = (long) (input.retainedDeviceMemoryBytes() * ((double) retainedPositionCount / input.positionCount()));
        long gatherMapBytes = (long) input.positionCount() * Integer.BYTES;
        return estimatedOutputDeviceBytes + gatherMapBytes;
    }

    public static long getOrderByMemoryUsage(DType[] sortTypes, long sortKeyBytes, long totalBytes, long rowCount)
    {
        // cuDF uses radix sort for a single fixed-width key, comparison sort otherwise.
        // Radix sort only needs working memory proportional to the key column (+ gather map);
        // comparison sort needs working memory proportional to the full table.
        long sortBytes;
        if (isRadixSortableSingleKey(sortTypes)) {
            sortBytes = 5 * sortKeyBytes + (totalBytes - sortKeyBytes);
        }
        else {
            sortBytes = (5 * totalBytes) / 2;
        }
        // Both sorts end by gathering the input table through an INT32 gather map, so the input,
        // the sorted output and the gather map are all live at once. For a wide table sorted on
        // a narrow key this exceeds the sort working memory.
        long gatherBytes = 2 * totalBytes + rowCount * Integer.BYTES;
        return Math.max(sortBytes, gatherBytes);
    }

    private static boolean isRadixSortableSingleKey(DType[] sortTypes)
    {
        if (sortTypes.length != 1) {
            return false;
        }
        DType type = sortTypes[0];
        return !type.isNestedType() && type.getTypeId() != DType.DTypeEnum.STRING;
    }
}
