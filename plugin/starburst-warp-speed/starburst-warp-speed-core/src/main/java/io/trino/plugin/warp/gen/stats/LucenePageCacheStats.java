
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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.metrics.WarpStatType;
import io.trino.plugin.warp.metrics.WarpStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName", "unused"})
public final class LucenePageCacheStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from lucenePageCache xml file for statistics and counters */
    private final String group;

    private final LongAdder lucene_page_cache_small_file_hit = new LongAdder();
    private final LongAdder lucene_page_cache_small_file_miss = new LongAdder();
    private final LongAdder lucene_page_cache_small_file_size = new LongAdder();
    private final LongAdder lucene_page_cache_big_file_hit = new LongAdder();
    private final LongAdder lucene_page_cache_big_file_miss = new LongAdder();
    private final LongAdder lucene_page_cache_big_file_size = new LongAdder();

    @JsonCreator
    public LucenePageCacheStats(@JsonProperty("group") String group)
    {
        super(createKey(group), WarpStatType.Worker);

        this.group = group;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
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
    public long getlucene_page_cache_small_file_size()
    {
        return lucene_page_cache_small_file_size.longValue();
    }

    public void inclucene_page_cache_small_file_size()
    {
        lucene_page_cache_small_file_size.increment();
    }

    public void addlucene_page_cache_small_file_size(long val)
    {
        lucene_page_cache_small_file_size.add(val);
    }

    public void setlucene_page_cache_small_file_size(long val)
    {
        lucene_page_cache_small_file_size.reset();
        addlucene_page_cache_small_file_size(val);
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

    @JsonIgnore
    @Managed
    public long getlucene_page_cache_big_file_size()
    {
        return lucene_page_cache_big_file_size.longValue();
    }

    public void inclucene_page_cache_big_file_size()
    {
        lucene_page_cache_big_file_size.increment();
    }

    public void addlucene_page_cache_big_file_size(long val)
    {
        lucene_page_cache_big_file_size.add(val);
    }

    public void setlucene_page_cache_big_file_size(long val)
    {
        lucene_page_cache_big_file_size.reset();
        addlucene_page_cache_big_file_size(val);
    }

    public static LucenePageCacheStats create(String group)
    {
        return new LucenePageCacheStats(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner("_").add(group).toString();
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("lucene_page_cache_small_file_hit", lucene_page_cache_small_file_hit);
        ret.put("lucene_page_cache_small_file_miss", lucene_page_cache_small_file_miss);
        ret.put("lucene_page_cache_small_file_size", lucene_page_cache_small_file_size);
        ret.put("lucene_page_cache_big_file_hit", lucene_page_cache_big_file_hit);
        ret.put("lucene_page_cache_big_file_miss", lucene_page_cache_big_file_miss);
        ret.put("lucene_page_cache_big_file_size", lucene_page_cache_big_file_size);

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
        this.lucene_page_cache_small_file_size.add(other.lucene_page_cache_small_file_size.longValue());
        this.lucene_page_cache_big_file_hit.add(other.lucene_page_cache_big_file_hit.longValue());
        this.lucene_page_cache_big_file_miss.add(other.lucene_page_cache_big_file_miss.longValue());
        this.lucene_page_cache_big_file_size.add(other.lucene_page_cache_big_file_size.longValue());
    }

    @Override
    public void reset()
    {
        lucene_page_cache_small_file_hit.reset();
        lucene_page_cache_small_file_miss.reset();
        lucene_page_cache_small_file_size.reset();
        lucene_page_cache_big_file_hit.reset();
        lucene_page_cache_big_file_miss.reset();
        lucene_page_cache_big_file_size.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        res.put(getJmxKey() + ":lucene_page_cache_small_file_hit", lucene_page_cache_small_file_hit.longValue());
        res.put(getJmxKey() + ":lucene_page_cache_small_file_miss", lucene_page_cache_small_file_miss.longValue());
        res.put(getJmxKey() + ":lucene_page_cache_small_file_size", lucene_page_cache_small_file_size.longValue());
        res.put(getJmxKey() + ":lucene_page_cache_big_file_hit", lucene_page_cache_big_file_hit.longValue());
        res.put(getJmxKey() + ":lucene_page_cache_big_file_miss", lucene_page_cache_big_file_miss.longValue());
        res.put(getJmxKey() + ":lucene_page_cache_big_file_size", lucene_page_cache_big_file_size.longValue());
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
        Map<String, Long> res = new HashMap<>();
        res.put("lucene_page_cache_small_file_size", getlucene_page_cache_small_file_size());
        res.put("lucene_page_cache_big_file_size", getlucene_page_cache_big_file_size());
        return res;
    }
}
