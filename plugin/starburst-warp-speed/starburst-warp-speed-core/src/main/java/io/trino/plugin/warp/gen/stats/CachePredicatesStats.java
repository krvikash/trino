
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
public final class CachePredicatesStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from cachePredicates xml file for statistics and counters */
    private final LongAdder in_use_tiny = new LongAdder();
    private final LongAdder in_use_small = new LongAdder();
    private final LongAdder in_use_medium = new LongAdder();
    private final LongAdder in_use_large = new LongAdder();
    private final LongAdder hit_tiny = new LongAdder();
    private final LongAdder hit_small = new LongAdder();
    private final LongAdder hit_medium = new LongAdder();
    private final LongAdder hit_large = new LongAdder();
    private final LongAdder miss_tiny = new LongAdder();
    private final LongAdder miss_small = new LongAdder();
    private final LongAdder miss_medium = new LongAdder();
    private final LongAdder miss_large = new LongAdder();
    private final LongAdder max_tiny = new LongAdder();
    private final LongAdder max_small = new LongAdder();
    private final LongAdder max_medium = new LongAdder();
    private final LongAdder max_large = new LongAdder();
    private final LongAdder size1_minus = new LongAdder();
    private final LongAdder size1_size2 = new LongAdder();
    private final LongAdder size2_size3 = new LongAdder();
    private final LongAdder size3_size4 = new LongAdder();
    private final LongAdder size4_size5 = new LongAdder();
    private final LongAdder size5_size6 = new LongAdder();
    private final LongAdder size6_size7 = new LongAdder();
    private final LongAdder size7_size8 = new LongAdder();
    private final LongAdder size8_size9 = new LongAdder();
    private final LongAdder size9_plus = new LongAdder();

    @JsonCreator
    public CachePredicatesStats()
    {
        super(createKey());
    }

    @JsonIgnore
    @Managed
    public long getin_use_tiny()
    {
        return in_use_tiny.longValue();
    }

    public void incin_use_tiny()
    {
        in_use_tiny.increment();
    }

    public void addin_use_tiny(long val)
    {
        in_use_tiny.add(val);
    }

    public void setin_use_tiny(long val)
    {
        in_use_tiny.reset();
        addin_use_tiny(val);
    }

    @JsonIgnore
    @Managed
    public long getin_use_small()
    {
        return in_use_small.longValue();
    }

    public void incin_use_small()
    {
        in_use_small.increment();
    }

    public void addin_use_small(long val)
    {
        in_use_small.add(val);
    }

    public void setin_use_small(long val)
    {
        in_use_small.reset();
        addin_use_small(val);
    }

    @JsonIgnore
    @Managed
    public long getin_use_medium()
    {
        return in_use_medium.longValue();
    }

    public void incin_use_medium()
    {
        in_use_medium.increment();
    }

    public void addin_use_medium(long val)
    {
        in_use_medium.add(val);
    }

    public void setin_use_medium(long val)
    {
        in_use_medium.reset();
        addin_use_medium(val);
    }

    @JsonIgnore
    @Managed
    public long getin_use_large()
    {
        return in_use_large.longValue();
    }

    public void incin_use_large()
    {
        in_use_large.increment();
    }

    public void addin_use_large(long val)
    {
        in_use_large.add(val);
    }

    public void setin_use_large(long val)
    {
        in_use_large.reset();
        addin_use_large(val);
    }

    @JsonIgnore
    @Managed
    public long gethit_tiny()
    {
        return hit_tiny.longValue();
    }

    public void inchit_tiny()
    {
        hit_tiny.increment();
    }

    public void addhit_tiny(long val)
    {
        hit_tiny.add(val);
    }

    public void sethit_tiny(long val)
    {
        hit_tiny.reset();
        addhit_tiny(val);
    }

    @JsonIgnore
    @Managed
    public long gethit_small()
    {
        return hit_small.longValue();
    }

    public void inchit_small()
    {
        hit_small.increment();
    }

    public void addhit_small(long val)
    {
        hit_small.add(val);
    }

    public void sethit_small(long val)
    {
        hit_small.reset();
        addhit_small(val);
    }

    @JsonIgnore
    @Managed
    public long gethit_medium()
    {
        return hit_medium.longValue();
    }

    public void inchit_medium()
    {
        hit_medium.increment();
    }

    public void addhit_medium(long val)
    {
        hit_medium.add(val);
    }

    public void sethit_medium(long val)
    {
        hit_medium.reset();
        addhit_medium(val);
    }

    @JsonIgnore
    @Managed
    public long gethit_large()
    {
        return hit_large.longValue();
    }

    public void inchit_large()
    {
        hit_large.increment();
    }

    public void addhit_large(long val)
    {
        hit_large.add(val);
    }

    public void sethit_large(long val)
    {
        hit_large.reset();
        addhit_large(val);
    }

    @JsonIgnore
    @Managed
    public long getmiss_tiny()
    {
        return miss_tiny.longValue();
    }

    public void incmiss_tiny()
    {
        miss_tiny.increment();
    }

    public void addmiss_tiny(long val)
    {
        miss_tiny.add(val);
    }

    public void setmiss_tiny(long val)
    {
        miss_tiny.reset();
        addmiss_tiny(val);
    }

    @JsonIgnore
    @Managed
    public long getmiss_small()
    {
        return miss_small.longValue();
    }

    public void incmiss_small()
    {
        miss_small.increment();
    }

    public void addmiss_small(long val)
    {
        miss_small.add(val);
    }

    public void setmiss_small(long val)
    {
        miss_small.reset();
        addmiss_small(val);
    }

    @JsonIgnore
    @Managed
    public long getmiss_medium()
    {
        return miss_medium.longValue();
    }

    public void incmiss_medium()
    {
        miss_medium.increment();
    }

    public void addmiss_medium(long val)
    {
        miss_medium.add(val);
    }

    public void setmiss_medium(long val)
    {
        miss_medium.reset();
        addmiss_medium(val);
    }

    @JsonIgnore
    @Managed
    public long getmiss_large()
    {
        return miss_large.longValue();
    }

    public void incmiss_large()
    {
        miss_large.increment();
    }

    public void addmiss_large(long val)
    {
        miss_large.add(val);
    }

    public void setmiss_large(long val)
    {
        miss_large.reset();
        addmiss_large(val);
    }

    @JsonIgnore
    @Managed
    public long getmax_tiny()
    {
        return max_tiny.longValue();
    }

    public void incmax_tiny()
    {
        max_tiny.increment();
    }

    public void addmax_tiny(long val)
    {
        max_tiny.add(val);
    }

    public void setmax_tiny(long val)
    {
        max_tiny.reset();
        addmax_tiny(val);
    }

    @JsonIgnore
    @Managed
    public long getmax_small()
    {
        return max_small.longValue();
    }

    public void incmax_small()
    {
        max_small.increment();
    }

    public void addmax_small(long val)
    {
        max_small.add(val);
    }

    public void setmax_small(long val)
    {
        max_small.reset();
        addmax_small(val);
    }

    @JsonIgnore
    @Managed
    public long getmax_medium()
    {
        return max_medium.longValue();
    }

    public void incmax_medium()
    {
        max_medium.increment();
    }

    public void addmax_medium(long val)
    {
        max_medium.add(val);
    }

    public void setmax_medium(long val)
    {
        max_medium.reset();
        addmax_medium(val);
    }

    @JsonIgnore
    @Managed
    public long getmax_large()
    {
        return max_large.longValue();
    }

    public void incmax_large()
    {
        max_large.increment();
    }

    public void addmax_large(long val)
    {
        max_large.add(val);
    }

    public void setmax_large(long val)
    {
        max_large.reset();
        addmax_large(val);
    }

    @JsonIgnore
    @Managed
    public long getsize1_minus()
    {
        return size1_minus.longValue();
    }

    public void incsize1_minus()
    {
        size1_minus.increment();
    }

    public void addsize1_minus(long val)
    {
        size1_minus.add(val);
    }

    public void setsize1_minus(long val)
    {
        size1_minus.reset();
        addsize1_minus(val);
    }

    @JsonIgnore
    @Managed
    public long getsize1_size2()
    {
        return size1_size2.longValue();
    }

    public void incsize1_size2()
    {
        size1_size2.increment();
    }

    public void addsize1_size2(long val)
    {
        size1_size2.add(val);
    }

    public void setsize1_size2(long val)
    {
        size1_size2.reset();
        addsize1_size2(val);
    }

    @JsonIgnore
    @Managed
    public long getsize2_size3()
    {
        return size2_size3.longValue();
    }

    public void incsize2_size3()
    {
        size2_size3.increment();
    }

    public void addsize2_size3(long val)
    {
        size2_size3.add(val);
    }

    public void setsize2_size3(long val)
    {
        size2_size3.reset();
        addsize2_size3(val);
    }

    @JsonIgnore
    @Managed
    public long getsize3_size4()
    {
        return size3_size4.longValue();
    }

    public void incsize3_size4()
    {
        size3_size4.increment();
    }

    public void addsize3_size4(long val)
    {
        size3_size4.add(val);
    }

    public void setsize3_size4(long val)
    {
        size3_size4.reset();
        addsize3_size4(val);
    }

    @JsonIgnore
    @Managed
    public long getsize4_size5()
    {
        return size4_size5.longValue();
    }

    public void incsize4_size5()
    {
        size4_size5.increment();
    }

    public void addsize4_size5(long val)
    {
        size4_size5.add(val);
    }

    public void setsize4_size5(long val)
    {
        size4_size5.reset();
        addsize4_size5(val);
    }

    @JsonIgnore
    @Managed
    public long getsize5_size6()
    {
        return size5_size6.longValue();
    }

    public void incsize5_size6()
    {
        size5_size6.increment();
    }

    public void addsize5_size6(long val)
    {
        size5_size6.add(val);
    }

    public void setsize5_size6(long val)
    {
        size5_size6.reset();
        addsize5_size6(val);
    }

    @JsonIgnore
    @Managed
    public long getsize6_size7()
    {
        return size6_size7.longValue();
    }

    public void incsize6_size7()
    {
        size6_size7.increment();
    }

    public void addsize6_size7(long val)
    {
        size6_size7.add(val);
    }

    public void setsize6_size7(long val)
    {
        size6_size7.reset();
        addsize6_size7(val);
    }

    @JsonIgnore
    @Managed
    public long getsize7_size8()
    {
        return size7_size8.longValue();
    }

    public void incsize7_size8()
    {
        size7_size8.increment();
    }

    public void addsize7_size8(long val)
    {
        size7_size8.add(val);
    }

    public void setsize7_size8(long val)
    {
        size7_size8.reset();
        addsize7_size8(val);
    }

    @JsonIgnore
    @Managed
    public long getsize8_size9()
    {
        return size8_size9.longValue();
    }

    public void incsize8_size9()
    {
        size8_size9.increment();
    }

    public void addsize8_size9(long val)
    {
        size8_size9.add(val);
    }

    public void setsize8_size9(long val)
    {
        size8_size9.reset();
        addsize8_size9(val);
    }

    @JsonIgnore
    @Managed
    public long getsize9_plus()
    {
        return size9_plus.longValue();
    }

    public void incsize9_plus()
    {
        size9_plus.increment();
    }

    public void addsize9_plus(long val)
    {
        size9_plus.add(val);
    }

    public void setsize9_plus(long val)
    {
        size9_plus.reset();
        addsize9_plus(val);
    }

    public static CachePredicatesStats create()
    {
        return new CachePredicatesStats();
    }

    public static String createKey()
    {
        return "cachePredicates";
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("in_use_tiny", in_use_tiny);
        ret.put("in_use_small", in_use_small);
        ret.put("in_use_medium", in_use_medium);
        ret.put("in_use_large", in_use_large);
        ret.put("hit_tiny", hit_tiny);
        ret.put("hit_small", hit_small);
        ret.put("hit_medium", hit_medium);
        ret.put("hit_large", hit_large);
        ret.put("miss_tiny", miss_tiny);
        ret.put("miss_small", miss_small);
        ret.put("miss_medium", miss_medium);
        ret.put("miss_large", miss_large);
        ret.put("max_tiny", max_tiny);
        ret.put("max_small", max_small);
        ret.put("max_medium", max_medium);
        ret.put("max_large", max_large);
        ret.put("size1_minus", size1_minus);
        ret.put("size1_size2", size1_size2);
        ret.put("size2_size3", size2_size3);
        ret.put("size3_size4", size3_size4);
        ret.put("size4_size5", size4_size5);
        ret.put("size5_size6", size5_size6);
        ret.put("size6_size7", size6_size7);
        ret.put("size7_size8", size7_size8);
        ret.put("size8_size9", size8_size9);
        ret.put("size9_plus", size9_plus);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        CachePredicatesStats other = (CachePredicatesStats) warpStatsBase;
        this.in_use_tiny.add(other.in_use_tiny.longValue());
        this.in_use_small.add(other.in_use_small.longValue());
        this.in_use_medium.add(other.in_use_medium.longValue());
        this.in_use_large.add(other.in_use_large.longValue());
        this.hit_tiny.add(other.hit_tiny.longValue());
        this.hit_small.add(other.hit_small.longValue());
        this.hit_medium.add(other.hit_medium.longValue());
        this.hit_large.add(other.hit_large.longValue());
        this.miss_tiny.add(other.miss_tiny.longValue());
        this.miss_small.add(other.miss_small.longValue());
        this.miss_medium.add(other.miss_medium.longValue());
        this.miss_large.add(other.miss_large.longValue());
        this.max_tiny.add(other.max_tiny.longValue());
        this.max_small.add(other.max_small.longValue());
        this.max_medium.add(other.max_medium.longValue());
        this.max_large.add(other.max_large.longValue());
        this.size1_minus.add(other.size1_minus.longValue());
        this.size1_size2.add(other.size1_size2.longValue());
        this.size2_size3.add(other.size2_size3.longValue());
        this.size3_size4.add(other.size3_size4.longValue());
        this.size4_size5.add(other.size4_size5.longValue());
        this.size5_size6.add(other.size5_size6.longValue());
        this.size6_size7.add(other.size6_size7.longValue());
        this.size7_size8.add(other.size7_size8.longValue());
        this.size8_size9.add(other.size8_size9.longValue());
        this.size9_plus.add(other.size9_plus.longValue());
    }

    @Override
    public void reset()
    {
        in_use_tiny.reset();
        in_use_small.reset();
        in_use_medium.reset();
        in_use_large.reset();
        hit_tiny.reset();
        hit_small.reset();
        hit_medium.reset();
        hit_large.reset();
        miss_tiny.reset();
        miss_small.reset();
        miss_medium.reset();
        miss_large.reset();
        max_tiny.reset();
        max_small.reset();
        max_medium.reset();
        max_large.reset();
        size1_minus.reset();
        size1_size2.reset();
        size2_size3.reset();
        size3_size4.reset();
        size4_size5.reset();
        size5_size6.reset();
        size6_size7.reset();
        size7_size8.reset();
        size8_size9.reset();
        size9_plus.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("cachePredicates:in_use_tiny", in_use_tiny.longValue());
        res.put("cachePredicates:in_use_small", in_use_small.longValue());
        res.put("cachePredicates:in_use_medium", in_use_medium.longValue());
        res.put("cachePredicates:in_use_large", in_use_large.longValue());
        res.put("cachePredicates:hit_tiny", hit_tiny.longValue());
        res.put("cachePredicates:hit_small", hit_small.longValue());
        res.put("cachePredicates:hit_medium", hit_medium.longValue());
        res.put("cachePredicates:hit_large", hit_large.longValue());
        res.put("cachePredicates:miss_tiny", miss_tiny.longValue());
        res.put("cachePredicates:miss_small", miss_small.longValue());
        res.put("cachePredicates:miss_medium", miss_medium.longValue());
        res.put("cachePredicates:miss_large", miss_large.longValue());
        res.put("cachePredicates:max_tiny", max_tiny.longValue());
        res.put("cachePredicates:max_small", max_small.longValue());
        res.put("cachePredicates:max_medium", max_medium.longValue());
        res.put("cachePredicates:max_large", max_large.longValue());
        res.put("cachePredicates:size1_minus", size1_minus.longValue());
        res.put("cachePredicates:size1_size2", size1_size2.longValue());
        res.put("cachePredicates:size2_size3", size2_size3.longValue());
        res.put("cachePredicates:size3_size4", size3_size4.longValue());
        res.put("cachePredicates:size4_size5", size4_size5.longValue());
        res.put("cachePredicates:size5_size6", size5_size6.longValue());
        res.put("cachePredicates:size6_size7", size6_size7.longValue());
        res.put("cachePredicates:size7_size8", size7_size8.longValue());
        res.put("cachePredicates:size8_size9", size8_size9.longValue());
        res.put("cachePredicates:size9_plus", size9_plus.longValue());
        return res;
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("in_use_tiny", getin_use_tiny());
        res.put("in_use_small", getin_use_small());
        res.put("in_use_medium", getin_use_medium());
        res.put("in_use_large", getin_use_large());
        res.put("hit_tiny", gethit_tiny());
        res.put("hit_small", gethit_small());
        res.put("hit_medium", gethit_medium());
        res.put("hit_large", gethit_large());
        res.put("miss_tiny", getmiss_tiny());
        res.put("miss_small", getmiss_small());
        res.put("miss_medium", getmiss_medium());
        res.put("miss_large", getmiss_large());
        res.put("max_tiny", getmax_tiny());
        res.put("max_small", getmax_small());
        res.put("max_medium", getmax_medium());
        res.put("max_large", getmax_large());
        res.put("size1_minus", getsize1_minus());
        res.put("size1_size2", getsize1_size2());
        res.put("size2_size3", getsize2_size3());
        res.put("size3_size4", getsize3_size4());
        res.put("size4_size5", getsize4_size5());
        res.put("size5_size6", getsize5_size6());
        res.put("size6_size7", getsize6_size7());
        res.put("size7_size8", getsize7_size8());
        res.put("size8_size9", getsize8_size9());
        res.put("size9_plus", getsize9_plus());
        return res;
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        return new HashMap<>();
    }
}
