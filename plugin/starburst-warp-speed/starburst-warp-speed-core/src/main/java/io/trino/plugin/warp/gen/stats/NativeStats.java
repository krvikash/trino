
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
public final class NativeStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from native xml file for statistics and counters */
    private final LongAdder read_cache_md_chunk_hits = new LongAdder();
    private final LongAdder read_cache_md_basic_hits = new LongAdder();
    private final LongAdder read_cache_md_nulls_hits = new LongAdder();
    private final LongAdder read_cache_md_chunk_misses = new LongAdder();
    private final LongAdder read_cache_md_basic_misses = new LongAdder();
    private final LongAdder read_cache_md_nulls_misses = new LongAdder();
    private final LongAdder read_cache_index_hits = new LongAdder();
    private final LongAdder read_cache_index_misses = new LongAdder();
    private final LongAdder read_time_wait_nanos = new LongAdder();

    @JsonCreator
    public NativeStats()
    {
        super(createKey());
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_chunk_hits()
    {
        return read_cache_md_chunk_hits.longValue();
    }

    public void incread_cache_md_chunk_hits()
    {
        read_cache_md_chunk_hits.increment();
    }

    public void addread_cache_md_chunk_hits(long val)
    {
        read_cache_md_chunk_hits.add(val);
    }

    public void setread_cache_md_chunk_hits(long val)
    {
        read_cache_md_chunk_hits.reset();
        addread_cache_md_chunk_hits(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_basic_hits()
    {
        return read_cache_md_basic_hits.longValue();
    }

    public void incread_cache_md_basic_hits()
    {
        read_cache_md_basic_hits.increment();
    }

    public void addread_cache_md_basic_hits(long val)
    {
        read_cache_md_basic_hits.add(val);
    }

    public void setread_cache_md_basic_hits(long val)
    {
        read_cache_md_basic_hits.reset();
        addread_cache_md_basic_hits(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_nulls_hits()
    {
        return read_cache_md_nulls_hits.longValue();
    }

    public void incread_cache_md_nulls_hits()
    {
        read_cache_md_nulls_hits.increment();
    }

    public void addread_cache_md_nulls_hits(long val)
    {
        read_cache_md_nulls_hits.add(val);
    }

    public void setread_cache_md_nulls_hits(long val)
    {
        read_cache_md_nulls_hits.reset();
        addread_cache_md_nulls_hits(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_chunk_misses()
    {
        return read_cache_md_chunk_misses.longValue();
    }

    public void incread_cache_md_chunk_misses()
    {
        read_cache_md_chunk_misses.increment();
    }

    public void addread_cache_md_chunk_misses(long val)
    {
        read_cache_md_chunk_misses.add(val);
    }

    public void setread_cache_md_chunk_misses(long val)
    {
        read_cache_md_chunk_misses.reset();
        addread_cache_md_chunk_misses(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_basic_misses()
    {
        return read_cache_md_basic_misses.longValue();
    }

    public void incread_cache_md_basic_misses()
    {
        read_cache_md_basic_misses.increment();
    }

    public void addread_cache_md_basic_misses(long val)
    {
        read_cache_md_basic_misses.add(val);
    }

    public void setread_cache_md_basic_misses(long val)
    {
        read_cache_md_basic_misses.reset();
        addread_cache_md_basic_misses(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_md_nulls_misses()
    {
        return read_cache_md_nulls_misses.longValue();
    }

    public void incread_cache_md_nulls_misses()
    {
        read_cache_md_nulls_misses.increment();
    }

    public void addread_cache_md_nulls_misses(long val)
    {
        read_cache_md_nulls_misses.add(val);
    }

    public void setread_cache_md_nulls_misses(long val)
    {
        read_cache_md_nulls_misses.reset();
        addread_cache_md_nulls_misses(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_index_hits()
    {
        return read_cache_index_hits.longValue();
    }

    public void incread_cache_index_hits()
    {
        read_cache_index_hits.increment();
    }

    public void addread_cache_index_hits(long val)
    {
        read_cache_index_hits.add(val);
    }

    public void setread_cache_index_hits(long val)
    {
        read_cache_index_hits.reset();
        addread_cache_index_hits(val);
    }

    @JsonIgnore
    @Managed
    public long getread_cache_index_misses()
    {
        return read_cache_index_misses.longValue();
    }

    public void incread_cache_index_misses()
    {
        read_cache_index_misses.increment();
    }

    public void addread_cache_index_misses(long val)
    {
        read_cache_index_misses.add(val);
    }

    public void setread_cache_index_misses(long val)
    {
        read_cache_index_misses.reset();
        addread_cache_index_misses(val);
    }

    @JsonIgnore
    @Managed
    public long getread_time_wait_nanos()
    {
        return read_time_wait_nanos.longValue();
    }

    public void incread_time_wait_nanos()
    {
        read_time_wait_nanos.increment();
    }

    public void addread_time_wait_nanos(long val)
    {
        read_time_wait_nanos.add(val);
    }

    public void setread_time_wait_nanos(long val)
    {
        read_time_wait_nanos.reset();
        addread_time_wait_nanos(val);
    }

    public static NativeStats create()
    {
        return new NativeStats();
    }

    public static String createKey()
    {
        return "native";
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("read_cache_md_chunk_hits", read_cache_md_chunk_hits);
        ret.put("read_cache_md_basic_hits", read_cache_md_basic_hits);
        ret.put("read_cache_md_nulls_hits", read_cache_md_nulls_hits);
        ret.put("read_cache_md_chunk_misses", read_cache_md_chunk_misses);
        ret.put("read_cache_md_basic_misses", read_cache_md_basic_misses);
        ret.put("read_cache_md_nulls_misses", read_cache_md_nulls_misses);
        ret.put("read_cache_index_hits", read_cache_index_hits);
        ret.put("read_cache_index_misses", read_cache_index_misses);
        ret.put("read_time_wait_nanos", read_time_wait_nanos);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        NativeStats other = (NativeStats) warpStatsBase;
        this.read_cache_md_chunk_hits.add(other.read_cache_md_chunk_hits.longValue());
        this.read_cache_md_basic_hits.add(other.read_cache_md_basic_hits.longValue());
        this.read_cache_md_nulls_hits.add(other.read_cache_md_nulls_hits.longValue());
        this.read_cache_md_chunk_misses.add(other.read_cache_md_chunk_misses.longValue());
        this.read_cache_md_basic_misses.add(other.read_cache_md_basic_misses.longValue());
        this.read_cache_md_nulls_misses.add(other.read_cache_md_nulls_misses.longValue());
        this.read_cache_index_hits.add(other.read_cache_index_hits.longValue());
        this.read_cache_index_misses.add(other.read_cache_index_misses.longValue());
        this.read_time_wait_nanos.add(other.read_time_wait_nanos.longValue());
    }

    @Override
    public void reset()
    {
        read_cache_md_chunk_hits.reset();
        read_cache_md_basic_hits.reset();
        read_cache_md_nulls_hits.reset();
        read_cache_md_chunk_misses.reset();
        read_cache_md_basic_misses.reset();
        read_cache_md_nulls_misses.reset();
        read_cache_index_hits.reset();
        read_cache_index_misses.reset();
        read_time_wait_nanos.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        if (read_cache_md_chunk_hits.longValue() > 0) {
            res.put("native:read_cache_md_chunk_hits", read_cache_md_chunk_hits.longValue());
        }
        if (read_cache_md_basic_hits.longValue() > 0) {
            res.put("native:read_cache_md_basic_hits", read_cache_md_basic_hits.longValue());
        }
        if (read_cache_md_nulls_hits.longValue() > 0) {
            res.put("native:read_cache_md_nulls_hits", read_cache_md_nulls_hits.longValue());
        }
        if (read_cache_md_chunk_misses.longValue() > 0) {
            res.put("native:read_cache_md_chunk_misses", read_cache_md_chunk_misses.longValue());
        }
        if (read_cache_md_basic_misses.longValue() > 0) {
            res.put("native:read_cache_md_basic_misses", read_cache_md_basic_misses.longValue());
        }
        if (read_cache_md_nulls_misses.longValue() > 0) {
            res.put("native:read_cache_md_nulls_misses", read_cache_md_nulls_misses.longValue());
        }
        if (read_cache_index_hits.longValue() > 0) {
            res.put("native:read_cache_index_hits", read_cache_index_hits.longValue());
        }
        if (read_cache_index_misses.longValue() > 0) {
            res.put("native:read_cache_index_misses", read_cache_index_misses.longValue());
        }
        res.put("native:read_time_wait_nanos", read_time_wait_nanos.longValue());
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
