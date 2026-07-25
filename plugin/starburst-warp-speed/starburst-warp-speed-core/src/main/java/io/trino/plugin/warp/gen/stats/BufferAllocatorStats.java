
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
package io.trino.plugin.warp.gen.stats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.plugin.warp.metrics.WarpStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName", "unused"})
public final class BufferAllocatorStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from bufferAllocator xml file for statistics and counters */
    private final LongAdder predicate_buffer_tiny_alloc = new LongAdder();
    private final LongAdder predicate_buffer_small_alloc = new LongAdder();
    private final LongAdder predicate_buffer_medium_alloc = new LongAdder();
    private final LongAdder predicate_buffer_large_alloc = new LongAdder();

    @JsonCreator
    public BufferAllocatorStats()
    {
        super(createKey());
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_tiny_alloc()
    {
        return predicate_buffer_tiny_alloc.longValue();
    }

    public void incpredicate_buffer_tiny_alloc()
    {
        predicate_buffer_tiny_alloc.increment();
    }

    public void addpredicate_buffer_tiny_alloc(long val)
    {
        predicate_buffer_tiny_alloc.add(val);
    }

    public void setpredicate_buffer_tiny_alloc(long val)
    {
        predicate_buffer_tiny_alloc.reset();
        addpredicate_buffer_tiny_alloc(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_small_alloc()
    {
        return predicate_buffer_small_alloc.longValue();
    }

    public void incpredicate_buffer_small_alloc()
    {
        predicate_buffer_small_alloc.increment();
    }

    public void addpredicate_buffer_small_alloc(long val)
    {
        predicate_buffer_small_alloc.add(val);
    }

    public void setpredicate_buffer_small_alloc(long val)
    {
        predicate_buffer_small_alloc.reset();
        addpredicate_buffer_small_alloc(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_medium_alloc()
    {
        return predicate_buffer_medium_alloc.longValue();
    }

    public void incpredicate_buffer_medium_alloc()
    {
        predicate_buffer_medium_alloc.increment();
    }

    public void addpredicate_buffer_medium_alloc(long val)
    {
        predicate_buffer_medium_alloc.add(val);
    }

    public void setpredicate_buffer_medium_alloc(long val)
    {
        predicate_buffer_medium_alloc.reset();
        addpredicate_buffer_medium_alloc(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_large_alloc()
    {
        return predicate_buffer_large_alloc.longValue();
    }

    public void incpredicate_buffer_large_alloc()
    {
        predicate_buffer_large_alloc.increment();
    }

    public void addpredicate_buffer_large_alloc(long val)
    {
        predicate_buffer_large_alloc.add(val);
    }

    public void setpredicate_buffer_large_alloc(long val)
    {
        predicate_buffer_large_alloc.reset();
        addpredicate_buffer_large_alloc(val);
    }

    public static BufferAllocatorStats create()
    {
        return new BufferAllocatorStats();
    }

    public static String createKey()
    {
        return "BufferAllocator";
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("predicate_buffer_tiny_alloc", predicate_buffer_tiny_alloc);
        ret.put("predicate_buffer_small_alloc", predicate_buffer_small_alloc);
        ret.put("predicate_buffer_medium_alloc", predicate_buffer_medium_alloc);
        ret.put("predicate_buffer_large_alloc", predicate_buffer_large_alloc);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        BufferAllocatorStats other = (BufferAllocatorStats) warpStatsBase;
        this.predicate_buffer_tiny_alloc.add(other.predicate_buffer_tiny_alloc.longValue());
        this.predicate_buffer_small_alloc.add(other.predicate_buffer_small_alloc.longValue());
        this.predicate_buffer_medium_alloc.add(other.predicate_buffer_medium_alloc.longValue());
        this.predicate_buffer_large_alloc.add(other.predicate_buffer_large_alloc.longValue());
    }

    @Override
    public void reset()
    {
        predicate_buffer_tiny_alloc.reset();
        predicate_buffer_small_alloc.reset();
        predicate_buffer_medium_alloc.reset();
        predicate_buffer_large_alloc.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("BufferAllocator:predicate_buffer_tiny_alloc", predicate_buffer_tiny_alloc.longValue());
        res.put("BufferAllocator:predicate_buffer_small_alloc", predicate_buffer_small_alloc.longValue());
        res.put("BufferAllocator:predicate_buffer_medium_alloc", predicate_buffer_medium_alloc.longValue());
        res.put("BufferAllocator:predicate_buffer_large_alloc", predicate_buffer_large_alloc.longValue());
        return res;
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        return new HashMap<>();
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        return new HashMap<>();
    }
}
