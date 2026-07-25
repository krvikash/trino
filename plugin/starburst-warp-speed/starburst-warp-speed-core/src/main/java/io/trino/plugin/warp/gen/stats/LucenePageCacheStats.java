
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
public final class LucenePageCacheStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from lucenePageCache xml file for statistics and counters */
    private final LongAdder lucene_page_cache_small_file_hit = new LongAdder();
    private final LongAdder lucene_page_cache_small_file_miss = new LongAdder();
    private final LongAdder lucene_page_cache_big_file_hit = new LongAdder();
    private final LongAdder lucene_page_cache_big_file_miss = new LongAdder();

    @JsonCreator
    public LucenePageCacheStats()
    {
        super(createKey());
    }

    @JsonIgnore
    @Managed
    public long getlucene_page_cache_small_file_hit()
    {
        return lucene_page_cache_small_file_hit.longValue();
    }

    public void inclucene_page_cache_small_file_hit()
    {
        lucene_page_cache_small_file_hit.increment();
    }

    public void addlucene_page_cache_small_file_hit(long val)
    {
        lucene_page_cache_small_file_hit.add(val);
    }

    public void setlucene_page_cache_small_file_hit(long val)
    {
        lucene_page_cache_small_file_hit.reset();
        addlucene_page_cache_small_file_hit(val);
    }

    @JsonIgnore
    @Managed
    public long getlucene_page_cache_small_file_miss()
    {
        return lucene_page_cache_small_file_miss.longValue();
    }

    public void inclucene_page_cache_small_file_miss()
    {
        lucene_page_cache_small_file_miss.increment();
    }

    public void addlucene_page_cache_small_file_miss(long val)
    {
        lucene_page_cache_small_file_miss.add(val);
    }

    public void setlucene_page_cache_small_file_miss(long val)
    {
        lucene_page_cache_small_file_miss.reset();
        addlucene_page_cache_small_file_miss(val);
    }

    @JsonIgnore
    @Managed
    public long getlucene_page_cache_big_file_hit()
    {
        return lucene_page_cache_big_file_hit.longValue();
    }

    public void inclucene_page_cache_big_file_hit()
    {
        lucene_page_cache_big_file_hit.increment();
    }

    public void addlucene_page_cache_big_file_hit(long val)
    {
        lucene_page_cache_big_file_hit.add(val);
    }

    public void setlucene_page_cache_big_file_hit(long val)
    {
        lucene_page_cache_big_file_hit.reset();
        addlucene_page_cache_big_file_hit(val);
    }

    @JsonIgnore
    @Managed
    public long getlucene_page_cache_big_file_miss()
    {
        return lucene_page_cache_big_file_miss.longValue();
    }

    public void inclucene_page_cache_big_file_miss()
    {
        lucene_page_cache_big_file_miss.increment();
    }

    public void addlucene_page_cache_big_file_miss(long val)
    {
        lucene_page_cache_big_file_miss.add(val);
    }

    public void setlucene_page_cache_big_file_miss(long val)
    {
        lucene_page_cache_big_file_miss.reset();
        addlucene_page_cache_big_file_miss(val);
    }

    public static LucenePageCacheStats create()
    {
        return new LucenePageCacheStats();
    }

    public static String createKey()
    {
        return "lucenePageCache";
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("lucene_page_cache_small_file_hit", lucene_page_cache_small_file_hit);
        ret.put("lucene_page_cache_small_file_miss", lucene_page_cache_small_file_miss);
        ret.put("lucene_page_cache_big_file_hit", lucene_page_cache_big_file_hit);
        ret.put("lucene_page_cache_big_file_miss", lucene_page_cache_big_file_miss);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        LucenePageCacheStats other = (LucenePageCacheStats) warpStatsBase;
        this.lucene_page_cache_small_file_hit.add(other.lucene_page_cache_small_file_hit.longValue());
        this.lucene_page_cache_small_file_miss.add(other.lucene_page_cache_small_file_miss.longValue());
        this.lucene_page_cache_big_file_hit.add(other.lucene_page_cache_big_file_hit.longValue());
        this.lucene_page_cache_big_file_miss.add(other.lucene_page_cache_big_file_miss.longValue());
    }

    @Override
    public void reset()
    {
        lucene_page_cache_small_file_hit.reset();
        lucene_page_cache_small_file_miss.reset();
        lucene_page_cache_big_file_hit.reset();
        lucene_page_cache_big_file_miss.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        return res;
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("lucene_page_cache_small_file_hit", getlucene_page_cache_small_file_hit());
        res.put("lucene_page_cache_small_file_miss", getlucene_page_cache_small_file_miss());
        res.put("lucene_page_cache_big_file_hit", getlucene_page_cache_big_file_hit());
        res.put("lucene_page_cache_big_file_miss", getlucene_page_cache_big_file_miss());
        return res;
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        return new HashMap<>();
    }
}
