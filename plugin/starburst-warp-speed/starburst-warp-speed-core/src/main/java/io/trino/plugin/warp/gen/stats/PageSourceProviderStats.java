
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
public final class PageSourceProviderStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from pageSourceProvider xml file for statistics and counters */
    private final String group;

    private final LongAdder total_splits = new LongAdder();
    private final LongAdder static_filtered_splits = new LongAdder();
    private final LongAdder dynamic_filtered_splits = new LongAdder();
    private final LongAdder skipped_splits = new LongAdder();

    @JsonCreator
    public PageSourceProviderStats(@JsonProperty("group") String group)
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
    public long gettotal_splits()
    {
        return total_splits.longValue();
    }

    public void inctotal_splits()
    {
        total_splits.increment();
    }

    public void addtotal_splits(long val)
    {
        total_splits.add(val);
    }

    public void settotal_splits(long val)
    {
        total_splits.reset();
        addtotal_splits(val);
    }

    @JsonIgnore
    @Managed
    public long getstatic_filtered_splits()
    {
        return static_filtered_splits.longValue();
    }

    public void incstatic_filtered_splits()
    {
        static_filtered_splits.increment();
    }

    public void addstatic_filtered_splits(long val)
    {
        static_filtered_splits.add(val);
    }

    public void setstatic_filtered_splits(long val)
    {
        static_filtered_splits.reset();
        addstatic_filtered_splits(val);
    }

    @JsonIgnore
    @Managed
    public long getdynamic_filtered_splits()
    {
        return dynamic_filtered_splits.longValue();
    }

    public void incdynamic_filtered_splits()
    {
        dynamic_filtered_splits.increment();
    }

    public void adddynamic_filtered_splits(long val)
    {
        dynamic_filtered_splits.add(val);
    }

    public void setdynamic_filtered_splits(long val)
    {
        dynamic_filtered_splits.reset();
        adddynamic_filtered_splits(val);
    }

    @JsonIgnore
    @Managed
    public long getskipped_splits()
    {
        return skipped_splits.longValue();
    }

    public void incskipped_splits()
    {
        skipped_splits.increment();
    }

    public void addskipped_splits(long val)
    {
        skipped_splits.add(val);
    }

    public void setskipped_splits(long val)
    {
        skipped_splits.reset();
        addskipped_splits(val);
    }

    public static PageSourceProviderStats create(String group)
    {
        return new PageSourceProviderStats(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner("_").add(group).toString();
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("total_splits", total_splits);
        ret.put("static_filtered_splits", static_filtered_splits);
        ret.put("dynamic_filtered_splits", dynamic_filtered_splits);
        ret.put("skipped_splits", skipped_splits);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        PageSourceProviderStats other = (PageSourceProviderStats) warpStatsBase;
        this.total_splits.add(other.total_splits.longValue());
        this.static_filtered_splits.add(other.static_filtered_splits.longValue());
        this.dynamic_filtered_splits.add(other.dynamic_filtered_splits.longValue());
        this.skipped_splits.add(other.skipped_splits.longValue());
    }

    @Override
    public void reset()
    {
        total_splits.reset();
        static_filtered_splits.reset();
        dynamic_filtered_splits.reset();
        skipped_splits.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        res.put(getJmxKey() + ":total_splits", total_splits.longValue());
        res.put(getJmxKey() + ":static_filtered_splits", static_filtered_splits.longValue());
        res.put(getJmxKey() + ":dynamic_filtered_splits", dynamic_filtered_splits.longValue());
        res.put(getJmxKey() + ":skipped_splits", skipped_splits.longValue());
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
