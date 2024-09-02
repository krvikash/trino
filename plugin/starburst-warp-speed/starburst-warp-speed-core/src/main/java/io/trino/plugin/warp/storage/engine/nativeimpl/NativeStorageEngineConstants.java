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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

@Singleton
public class NativeStorageEngineConstants
        implements StorageEngineConstants
{
    private static final Logger logger = Logger.get(NativeStorageEngineConstants.class);

    // page size
    private final int pageSizeShift;                 // native layer page size shift
    private final int pageSize;                      // native layer page size
    private final int pageSizeMask;                  // mask for page alignment
    private final int pageOffsetMask;                // mask for in page offset
    // buffer
    private final int recordBufferMaxSize;
    private final int indexChunkMaxSize;
    private final int queryStringNullValueSize;
    private final int chunksMapSize;
    private final int chunkHeaderMaxSize;
    private final int warmupDataTempBufferSize;
    private final int warmupIndexTempBufferSize;
    private final int maxWeContextSize;
    // varchar
    private final int maxRecLen;
    private final int fixedLengthStringLimit;
    private final int varcharMaxLen;
    private final int varlenExtMark;
    private final int varlenMarkEnd;
    private final int varlenSkiplistGranularity;
    private final int varlenExtLimit;
    private final int varlenExtRecordHeaderSize;
    // tx and memory resources
    private final int numBundles;
    private final int bundleNonCollectSize;
    // chunk
    private final int chunkSizeShift;
    private final int matchCollectBufferSize;
    private final int maxChunksInRange;
    private final int matchCollectNumIds;
    private final int maxMatchColumns;
    private final int maxLuceneColumnsInBundle;
    private final int matchTxSize;
    // lucene
    private final int luceneSmallJufferSize;
    private final int luceneBigJufferSize;

    @Inject
    public NativeStorageEngineConstants()
    {
        try {
            SymbolLookup libraryHandle = SymbolLookup.loaderLookup();

            // page size
            pageSizeShift = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_page_size_shift");
            pageSize = 1 << pageSizeShift;
            pageOffsetMask = pageSize - 1;
            pageSizeMask = ~pageOffsetMask;

            // buffers
            recordBufferMaxSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_record_buffer_max_size");
            indexChunkMaxSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_index_chunk_max_size");
            queryStringNullValueSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_query_string_null_value_size");
            chunksMapSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_chunks_map_size");
            chunkHeaderMaxSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_chunk_header_max_size");
            warmupDataTempBufferSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_warmup_data_temp_buffer_size");
            warmupIndexTempBufferSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_warmup_index_temp_buffer_size");
            maxWeContextSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_max_we_context_size");

            // varchar
            maxRecLen = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_max_rec_len");
            fixedLengthStringLimit = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_fixed_length_string_limit");
            varcharMaxLen = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_max_len");
            varlenExtMark = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_mark_ext");
            varlenMarkEnd = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_mark_end");
            varlenSkiplistGranularity = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_md_granularity");
            varlenExtLimit = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_ext_limit");
            varlenExtRecordHeaderSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_varlen_ext_record_header_size");

            // tx and memory
            numBundles = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_num_bundles");
            bundleNonCollectSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_bundle_non_collect_size");

            // chunk
            chunkSizeShift = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_chunk_size_shift");
            matchCollectBufferSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_match_collect_buffer_size");
            maxChunksInRange = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_max_chunks_in_range");
            matchCollectNumIds = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_match_collect_num_ids");
            maxMatchColumns = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_max_match_columns");
            maxLuceneColumnsInBundle = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_max_lucene_columns_in_bundle");
            matchTxSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_match_tx_size");

            // lucene
            luceneBigJufferSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_lucene_big_juffer_size");
            luceneSmallJufferSize = getWarpSpeedConstant(libraryHandle, "warp_speed_constants_get_lucene_small_juffer_size");
        }
        catch (Throwable t) {
            logger.error(t, "failed to retrieve constants from native library");
            throw new RuntimeException("failed to initialize NativeStorageEngineConstants");
        }
    }

    private int getWarpSpeedConstant(SymbolLookup libraryHandle, String name)
            throws Throwable
    {
        return (int) Linker.nativeLinker().downcallHandle(libraryHandle.find(name).orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_INT)).invokeExact();
    }

    @Override
    public int getMaxRecLen()
    {
        return maxRecLen;
    }

    @Override
    public int getPageOffsetMask()
    {
        return pageOffsetMask;
    }

    @Override
    public int getPageSize()
    {
        return pageSize;
    }

    @Override
    public int getPageSizeMask()
    {
        return pageSizeMask;
    }

    @Override
    public int getPageSizeShift()
    {
        return pageSizeShift;
    }

    @Override
    public int getRecordBufferMaxSize()
    {
        return recordBufferMaxSize;
    }

    @Override
    public int getIndexChunkMaxSize()
    {
        return indexChunkMaxSize;
    }

    @Override
    public int getQueryStringNullValueSize()
    {
        return queryStringNullValueSize;
    }

    @Override
    public int getChunksMapSize()
    {
        return chunksMapSize;
    }

    @Override
    public int getChunkHeaderMaxSize()
    {
        return chunkHeaderMaxSize;
    }

    @Override
    public int getWarmupDataTempBufferSize()
    {
        return warmupDataTempBufferSize;
    }

    @Override
    public int getWarmupIndexTempBufferSize()
    {
        return warmupIndexTempBufferSize;
    }

    @Override
    public int getMaxWeContextSize()
    {
        return maxWeContextSize;
    }

    @Override
    public int getFixedLengthStringLimit()
    {
        return fixedLengthStringLimit;
    }

    @Override
    public int getVarcharMaxLen()
    {
        return varcharMaxLen;
    }

    @Override
    public int getVarlenExtLimit()
    {
        return varlenExtLimit;
    }

    @Override
    public int getVarlenExtMark()
    {
        return varlenExtMark;
    }

    @Override
    public int getVarlenExtRecordHeaderSize()
    {
        return varlenExtRecordHeaderSize;
    }

    @Override
    public int getVarlenMarkEnd()
    {
        return varlenMarkEnd;
    }

    @Override
    public int getVarlenMdGranularity()
    {
        return varlenSkiplistGranularity;
    }

    @Override
    public int getNumBundles()
    {
        return numBundles;
    }

    @Override
    public int getBundleNonCollectSize()
    {
        return bundleNonCollectSize;
    }

    @Override
    public int getChunkSizeShift()
    {
        return chunkSizeShift;
    }

    @Override
    public int getMatchCollectBufferSize()
    {
        return matchCollectBufferSize;
    }

    @Override
    public int getMaxChunksInRange()
    {
        return maxChunksInRange;
    }

    @Override
    public int getMatchCollectNumIds()
    {
        return matchCollectNumIds;
    }

    @Override
    public int getMaxMatchColumns()
    {
        return maxMatchColumns;
    }

    @Override
    public int getMaxLuceneColumnsInBundle()
    {
        return maxLuceneColumnsInBundle;
    }

    @Override
    public int getMatchTxSize()
    {
        return matchTxSize;
    }

    @Override
    public int getLuceneSmallJufferSize()
    {
        return luceneSmallJufferSize;
    }

    @Override
    public int getLuceneBigJufferSize()
    {
        return luceneBigJufferSize;
    }
}
