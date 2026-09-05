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
package io.trino.plugin.warp.storage.read;

import io.trino.plugin.warp.gen.stats.NativeStats;
import io.trino.plugin.warp.storage.memory.ThreadArena;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class ReadStats
{
    static final StructLayout READ_STATS_LAYOUT;
    private static final long READ_STATS_OFFSET_CACHE_MD_CHUNK_HITS;
    private static final long READ_STATS_OFFSET_CACHE_MD_CHUNK_MISSES;
    private static final long READ_STATS_OFFSET_CACHE_MD_BASIC_HITS;
    private static final long READ_STATS_OFFSET_CACHE_MD_BASIC_MISSES;
    private static final long READ_STATS_OFFSET_CACHE_MD_NULLS_HITS;
    private static final long READ_STATS_OFFSET_CACHE_MD_NULLS_MISSES;
    private static final long READ_STATS_OFFSET_CACHE_INDEX_HITS;
    private static final long READ_STATS_OFFSET_CACHE_INDEX_MISSES;
    private static final long READ_STATS_OFFSET_UNCACHED_PAGES;
    private static final long READ_STATS_OFFSET_READ_TIME_WAIT_NANOS;

    private MemorySegment readStats;

    static {
        READ_STATS_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("cache_md_chunk_hits"),
                ValueLayout.JAVA_LONG.withName("cache_md_chunk_misses"),
                ValueLayout.JAVA_LONG.withName("cache_md_basic_hits"),
                ValueLayout.JAVA_LONG.withName("cache_md_basic_misses"),
                ValueLayout.JAVA_LONG.withName("cache_md_nulls_hits"),
                ValueLayout.JAVA_LONG.withName("cache_md_nulls_misses"),
                ValueLayout.JAVA_LONG.withName("cache_index_hits"),
                ValueLayout.JAVA_LONG.withName("cache_index_misses"),
                ValueLayout.JAVA_LONG.withName("uncached_pages"),
                ValueLayout.JAVA_LONG.withName("read_time_wait_nanos")).withName("storage_read_stats_t");
        READ_STATS_OFFSET_CACHE_MD_CHUNK_HITS = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_chunk_hits"));
        READ_STATS_OFFSET_CACHE_MD_CHUNK_MISSES = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_chunk_misses"));
        READ_STATS_OFFSET_CACHE_MD_BASIC_HITS = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_basic_hits"));
        READ_STATS_OFFSET_CACHE_MD_BASIC_MISSES = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_basic_misses"));
        READ_STATS_OFFSET_CACHE_MD_NULLS_HITS = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_nulls_hits"));
        READ_STATS_OFFSET_CACHE_MD_NULLS_MISSES = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_md_nulls_misses"));
        READ_STATS_OFFSET_CACHE_INDEX_HITS = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_index_hits"));
        READ_STATS_OFFSET_CACHE_INDEX_MISSES = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("cache_index_misses"));
        READ_STATS_OFFSET_UNCACHED_PAGES = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("uncached_pages"));
        READ_STATS_OFFSET_READ_TIME_WAIT_NANOS = READ_STATS_LAYOUT.byteOffset(PathElement.groupElement("read_time_wait_nanos"));
    }

    public ReadStats(ThreadArena arena)
    {
        this.readStats = arena.allocate(READ_STATS_LAYOUT.byteSize(), ValueLayout.JAVA_LONG.byteSize());
    }

    public MemorySegment getMemory()
    {
        return readStats;
    }

    int fillStats(NativeStats nativeStats)
    {
        int totalReadPages = 0;
        long hits = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_CHUNK_HITS);
        nativeStats.addread_cache_md_chunk_hits(hits);
        totalReadPages += (int) hits;
        long misses = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_CHUNK_MISSES);
        nativeStats.addread_cache_md_chunk_misses(misses);
        totalReadPages += (int) misses;
        hits = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_BASIC_HITS);
        nativeStats.addread_cache_md_basic_hits(hits);
        totalReadPages += (int) hits;
        misses = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_BASIC_MISSES);
        nativeStats.addread_cache_md_basic_misses(misses);
        totalReadPages += (int) misses;
        hits = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_NULLS_HITS);
        nativeStats.addread_cache_md_nulls_hits(hits);
        totalReadPages += (int) hits;
        misses = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_MD_NULLS_MISSES);
        nativeStats.addread_cache_md_nulls_misses(misses);
        totalReadPages += (int) misses;
        hits = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_INDEX_HITS);
        nativeStats.addread_cache_index_hits(hits);
        totalReadPages += (int) hits;
        misses = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_CACHE_INDEX_MISSES);
        nativeStats.addread_cache_index_misses(misses);
        totalReadPages += (int) misses;

        long uncachedPages = readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_UNCACHED_PAGES);
        nativeStats.addread_uncached_pages(uncachedPages);
        totalReadPages += (int) uncachedPages;

        nativeStats.addread_time_wait_nanos(readStats.get(ValueLayout.JAVA_LONG, READ_STATS_OFFSET_READ_TIME_WAIT_NANOS));

        return totalReadPages;
    }
}
