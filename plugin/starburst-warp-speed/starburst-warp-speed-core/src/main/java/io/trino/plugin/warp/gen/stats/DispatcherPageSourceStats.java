
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
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class DispatcherPageSourceStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from dispatcherPageSource xml file for statistics and counters */
    private final String group;

    private final LongAdder cached_files = new LongAdder();
    private final LongAdder df_splits = new LongAdder();
    private final LongAdder cached_warp_success_files = new LongAdder();
    private final LongAdder cached_warp_failed_files = new LongAdder();
    private final LongAdder cached_warp_failed_pages = new LongAdder();
    private final LongAdder cached_proxied_files = new LongAdder();
    private final LongAdder cached_total_rows = new LongAdder();
    private final LongAdder cached_read_rows = new LongAdder();
    private final LongAdder warp_match_columns = new LongAdder();
    private final LongAdder warp_match_on_simplified_domain = new LongAdder();
    private final LongAdder warp_collect_columns = new LongAdder();
    private final LongAdder warp_match_collect_columns = new LongAdder();
    private final LongAdder warp_mapped_match_collect_columns = new LongAdder();
    private final LongAdder warp_prefilled_collect_columns = new LongAdder();
    private final LongAdder empty_collect_columns = new LongAdder();
    private final LongAdder external_match_columns = new LongAdder();
    private final LongAdder external_collect_columns = new LongAdder();
    private final LongAdder filtered_by_predicate = new LongAdder();
    private final LongAdder non_trivial_alternative_chosen = new LongAdder();
    private final LongAdder empty_row_group = new LongAdder();
    private final LongAdder transformed_column = new LongAdder();
    private final LongAdder empty_page_source = new LongAdder();
    private final LongAdder warp_cache_manager = new LongAdder();
    private final LongAdder skip_warp_cache_manager = new LongAdder();
    private final LongAdder proxied_pages = new LongAdder();
    private final LongAdder proxied_time = new LongAdder();
    private final LongAdder proxied_loaded_pages = new LongAdder();
    private final LongAdder proxied_loaded_pages_time = new LongAdder();
    private final LongAdder proxied_loaded_pages_bytes = new LongAdder();
    private final LongAdder lucene_execution_time_Count = new LongAdder();
    private final LongAdder lucene_execution_time = new LongAdder();
    private final LongAdder execution_time_Count = new LongAdder();
    private final LongAdder execution_time = new LongAdder();

    @JsonCreator
    public DispatcherPageSourceStats(@JsonProperty("group") String group)
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
    public long getcached_files()
    {
        return cached_files.longValue();
    }

    public void inccached_files()
    {
        cached_files.increment();
    }

    public void addcached_files(long val)
    {
        cached_files.add(val);
    }

    public void setcached_files(long val)
    {
        cached_files.reset();
        addcached_files(val);
    }

    @JsonIgnore
    @Managed
    public long getdf_splits()
    {
        return df_splits.longValue();
    }

    public void incdf_splits()
    {
        df_splits.increment();
    }

    public void adddf_splits(long val)
    {
        df_splits.add(val);
    }

    public void setdf_splits(long val)
    {
        df_splits.reset();
        adddf_splits(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_warp_success_files()
    {
        return cached_warp_success_files.longValue();
    }

    public void inccached_warp_success_files()
    {
        cached_warp_success_files.increment();
    }

    public void addcached_warp_success_files(long val)
    {
        cached_warp_success_files.add(val);
    }

    public void setcached_warp_success_files(long val)
    {
        cached_warp_success_files.reset();
        addcached_warp_success_files(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_warp_failed_files()
    {
        return cached_warp_failed_files.longValue();
    }

    public void inccached_warp_failed_files()
    {
        cached_warp_failed_files.increment();
    }

    public void addcached_warp_failed_files(long val)
    {
        cached_warp_failed_files.add(val);
    }

    public void setcached_warp_failed_files(long val)
    {
        cached_warp_failed_files.reset();
        addcached_warp_failed_files(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_warp_failed_pages()
    {
        return cached_warp_failed_pages.longValue();
    }

    public void inccached_warp_failed_pages()
    {
        cached_warp_failed_pages.increment();
    }

    public void addcached_warp_failed_pages(long val)
    {
        cached_warp_failed_pages.add(val);
    }

    public void setcached_warp_failed_pages(long val)
    {
        cached_warp_failed_pages.reset();
        addcached_warp_failed_pages(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_proxied_files()
    {
        return cached_proxied_files.longValue();
    }

    public void inccached_proxied_files()
    {
        cached_proxied_files.increment();
    }

    public void addcached_proxied_files(long val)
    {
        cached_proxied_files.add(val);
    }

    public void setcached_proxied_files(long val)
    {
        cached_proxied_files.reset();
        addcached_proxied_files(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_total_rows()
    {
        return cached_total_rows.longValue();
    }

    public void inccached_total_rows()
    {
        cached_total_rows.increment();
    }

    public void addcached_total_rows(long val)
    {
        cached_total_rows.add(val);
    }

    public void setcached_total_rows(long val)
    {
        cached_total_rows.reset();
        addcached_total_rows(val);
    }

    @JsonIgnore
    @Managed
    public long getcached_read_rows()
    {
        return cached_read_rows.longValue();
    }

    public void inccached_read_rows()
    {
        cached_read_rows.increment();
    }

    public void addcached_read_rows(long val)
    {
        cached_read_rows.add(val);
    }

    public void setcached_read_rows(long val)
    {
        cached_read_rows.reset();
        addcached_read_rows(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_match_columns()
    {
        return warp_match_columns.longValue();
    }

    public void incwarp_match_columns()
    {
        warp_match_columns.increment();
    }

    public void addwarp_match_columns(long val)
    {
        warp_match_columns.add(val);
    }

    public void setwarp_match_columns(long val)
    {
        warp_match_columns.reset();
        addwarp_match_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_match_on_simplified_domain()
    {
        return warp_match_on_simplified_domain.longValue();
    }

    public void incwarp_match_on_simplified_domain()
    {
        warp_match_on_simplified_domain.increment();
    }

    public void addwarp_match_on_simplified_domain(long val)
    {
        warp_match_on_simplified_domain.add(val);
    }

    public void setwarp_match_on_simplified_domain(long val)
    {
        warp_match_on_simplified_domain.reset();
        addwarp_match_on_simplified_domain(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_collect_columns()
    {
        return warp_collect_columns.longValue();
    }

    public void incwarp_collect_columns()
    {
        warp_collect_columns.increment();
    }

    public void addwarp_collect_columns(long val)
    {
        warp_collect_columns.add(val);
    }

    public void setwarp_collect_columns(long val)
    {
        warp_collect_columns.reset();
        addwarp_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_match_collect_columns()
    {
        return warp_match_collect_columns.longValue();
    }

    public void incwarp_match_collect_columns()
    {
        warp_match_collect_columns.increment();
    }

    public void addwarp_match_collect_columns(long val)
    {
        warp_match_collect_columns.add(val);
    }

    public void setwarp_match_collect_columns(long val)
    {
        warp_match_collect_columns.reset();
        addwarp_match_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_mapped_match_collect_columns()
    {
        return warp_mapped_match_collect_columns.longValue();
    }

    public void incwarp_mapped_match_collect_columns()
    {
        warp_mapped_match_collect_columns.increment();
    }

    public void addwarp_mapped_match_collect_columns(long val)
    {
        warp_mapped_match_collect_columns.add(val);
    }

    public void setwarp_mapped_match_collect_columns(long val)
    {
        warp_mapped_match_collect_columns.reset();
        addwarp_mapped_match_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_prefilled_collect_columns()
    {
        return warp_prefilled_collect_columns.longValue();
    }

    public void incwarp_prefilled_collect_columns()
    {
        warp_prefilled_collect_columns.increment();
    }

    public void addwarp_prefilled_collect_columns(long val)
    {
        warp_prefilled_collect_columns.add(val);
    }

    public void setwarp_prefilled_collect_columns(long val)
    {
        warp_prefilled_collect_columns.reset();
        addwarp_prefilled_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getempty_collect_columns()
    {
        return empty_collect_columns.longValue();
    }

    public void incempty_collect_columns()
    {
        empty_collect_columns.increment();
    }

    public void addempty_collect_columns(long val)
    {
        empty_collect_columns.add(val);
    }

    public void setempty_collect_columns(long val)
    {
        empty_collect_columns.reset();
        addempty_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getexternal_match_columns()
    {
        return external_match_columns.longValue();
    }

    public void incexternal_match_columns()
    {
        external_match_columns.increment();
    }

    public void addexternal_match_columns(long val)
    {
        external_match_columns.add(val);
    }

    public void setexternal_match_columns(long val)
    {
        external_match_columns.reset();
        addexternal_match_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getexternal_collect_columns()
    {
        return external_collect_columns.longValue();
    }

    public void incexternal_collect_columns()
    {
        external_collect_columns.increment();
    }

    public void addexternal_collect_columns(long val)
    {
        external_collect_columns.add(val);
    }

    public void setexternal_collect_columns(long val)
    {
        external_collect_columns.reset();
        addexternal_collect_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getfiltered_by_predicate()
    {
        return filtered_by_predicate.longValue();
    }

    public void incfiltered_by_predicate()
    {
        filtered_by_predicate.increment();
    }

    public void addfiltered_by_predicate(long val)
    {
        filtered_by_predicate.add(val);
    }

    public void setfiltered_by_predicate(long val)
    {
        filtered_by_predicate.reset();
        addfiltered_by_predicate(val);
    }

    @JsonIgnore
    @Managed
    public long getnon_trivial_alternative_chosen()
    {
        return non_trivial_alternative_chosen.longValue();
    }

    public void incnon_trivial_alternative_chosen()
    {
        non_trivial_alternative_chosen.increment();
    }

    public void addnon_trivial_alternative_chosen(long val)
    {
        non_trivial_alternative_chosen.add(val);
    }

    public void setnon_trivial_alternative_chosen(long val)
    {
        non_trivial_alternative_chosen.reset();
        addnon_trivial_alternative_chosen(val);
    }

    @JsonIgnore
    @Managed
    public long getempty_row_group()
    {
        return empty_row_group.longValue();
    }

    public void incempty_row_group()
    {
        empty_row_group.increment();
    }

    public void addempty_row_group(long val)
    {
        empty_row_group.add(val);
    }

    public void setempty_row_group(long val)
    {
        empty_row_group.reset();
        addempty_row_group(val);
    }

    @JsonIgnore
    @Managed
    public long gettransformed_column()
    {
        return transformed_column.longValue();
    }

    public void inctransformed_column()
    {
        transformed_column.increment();
    }

    public void addtransformed_column(long val)
    {
        transformed_column.add(val);
    }

    public void settransformed_column(long val)
    {
        transformed_column.reset();
        addtransformed_column(val);
    }

    @JsonIgnore
    @Managed
    public long getempty_page_source()
    {
        return empty_page_source.longValue();
    }

    public void incempty_page_source()
    {
        empty_page_source.increment();
    }

    public void addempty_page_source(long val)
    {
        empty_page_source.add(val);
    }

    public void setempty_page_source(long val)
    {
        empty_page_source.reset();
        addempty_page_source(val);
    }

    @JsonIgnore
    @Managed
    public long getwarp_cache_manager()
    {
        return warp_cache_manager.longValue();
    }

    public void incwarp_cache_manager()
    {
        warp_cache_manager.increment();
    }

    public void addwarp_cache_manager(long val)
    {
        warp_cache_manager.add(val);
    }

    public void setwarp_cache_manager(long val)
    {
        warp_cache_manager.reset();
        addwarp_cache_manager(val);
    }

    @JsonIgnore
    @Managed
    public long getskip_warp_cache_manager()
    {
        return skip_warp_cache_manager.longValue();
    }

    public void incskip_warp_cache_manager()
    {
        skip_warp_cache_manager.increment();
    }

    public void addskip_warp_cache_manager(long val)
    {
        skip_warp_cache_manager.add(val);
    }

    public void setskip_warp_cache_manager(long val)
    {
        skip_warp_cache_manager.reset();
        addskip_warp_cache_manager(val);
    }

    @JsonIgnore
    @Managed
    public long getproxied_pages()
    {
        return proxied_pages.longValue();
    }

    public void incproxied_pages()
    {
        proxied_pages.increment();
    }

    public void addproxied_pages(long val)
    {
        proxied_pages.add(val);
    }

    public void setproxied_pages(long val)
    {
        proxied_pages.reset();
        addproxied_pages(val);
    }

    @JsonIgnore
    @Managed
    public long getproxied_time()
    {
        return proxied_time.longValue();
    }

    public void incproxied_time()
    {
        proxied_time.increment();
    }

    public void addproxied_time(long val)
    {
        proxied_time.add(val);
    }

    public void setproxied_time(long val)
    {
        proxied_time.reset();
        addproxied_time(val);
    }

    @JsonIgnore
    @Managed
    public long getproxied_loaded_pages()
    {
        return proxied_loaded_pages.longValue();
    }

    public void incproxied_loaded_pages()
    {
        proxied_loaded_pages.increment();
    }

    public void addproxied_loaded_pages(long val)
    {
        proxied_loaded_pages.add(val);
    }

    public void setproxied_loaded_pages(long val)
    {
        proxied_loaded_pages.reset();
        addproxied_loaded_pages(val);
    }

    @JsonIgnore
    @Managed
    public long getproxied_loaded_pages_time()
    {
        return proxied_loaded_pages_time.longValue();
    }

    public void incproxied_loaded_pages_time()
    {
        proxied_loaded_pages_time.increment();
    }

    public void addproxied_loaded_pages_time(long val)
    {
        proxied_loaded_pages_time.add(val);
    }

    public void setproxied_loaded_pages_time(long val)
    {
        proxied_loaded_pages_time.reset();
        addproxied_loaded_pages_time(val);
    }

    @JsonIgnore
    @Managed
    public long getproxied_loaded_pages_bytes()
    {
        return proxied_loaded_pages_bytes.longValue();
    }

    public void incproxied_loaded_pages_bytes()
    {
        proxied_loaded_pages_bytes.increment();
    }

    public void addproxied_loaded_pages_bytes(long val)
    {
        proxied_loaded_pages_bytes.add(val);
    }

    public void setproxied_loaded_pages_bytes(long val)
    {
        proxied_loaded_pages_bytes.reset();
        addproxied_loaded_pages_bytes(val);
    }

    @JsonIgnore
    @Managed
    public long getlucene_execution_time_Count()
    {
        return lucene_execution_time_Count.longValue();
    }

    @Managed
    public long getlucene_execution_time_Average()
    {
        if (lucene_execution_time_Count.longValue() == 0) {
            return 0;
        }
        return lucene_execution_time.longValue() / lucene_execution_time_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getlucene_execution_time()
    {
        return lucene_execution_time.longValue();
    }

    public void addlucene_execution_time(long val)
    {
        lucene_execution_time.add(val);
        lucene_execution_time_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getexecution_time_Count()
    {
        return execution_time_Count.longValue();
    }

    @Managed
    public long getexecution_time_Average()
    {
        if (execution_time_Count.longValue() == 0) {
            return 0;
        }
        return execution_time.longValue() / execution_time_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getexecution_time()
    {
        return execution_time.longValue();
    }

    public void addexecution_time(long val)
    {
        execution_time.add(val);
        execution_time_Count.add(1);
    }

    public static DispatcherPageSourceStats create(String group)
    {
        return new DispatcherPageSourceStats(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner(".").add(group).toString();
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("cached_files", cached_files);
        ret.put("df_splits", df_splits);
        ret.put("cached_warp_success_files", cached_warp_success_files);
        ret.put("cached_warp_failed_files", cached_warp_failed_files);
        ret.put("cached_warp_failed_pages", cached_warp_failed_pages);
        ret.put("cached_proxied_files", cached_proxied_files);
        ret.put("cached_total_rows", cached_total_rows);
        ret.put("cached_read_rows", cached_read_rows);
        ret.put("warp_match_columns", warp_match_columns);
        ret.put("warp_match_on_simplified_domain", warp_match_on_simplified_domain);
        ret.put("warp_collect_columns", warp_collect_columns);
        ret.put("warp_match_collect_columns", warp_match_collect_columns);
        ret.put("warp_mapped_match_collect_columns", warp_mapped_match_collect_columns);
        ret.put("warp_prefilled_collect_columns", warp_prefilled_collect_columns);
        ret.put("empty_collect_columns", empty_collect_columns);
        ret.put("external_match_columns", external_match_columns);
        ret.put("external_collect_columns", external_collect_columns);
        ret.put("filtered_by_predicate", filtered_by_predicate);
        ret.put("non_trivial_alternative_chosen", non_trivial_alternative_chosen);
        ret.put("empty_row_group", empty_row_group);
        ret.put("transformed_column", transformed_column);
        ret.put("empty_page_source", empty_page_source);
        ret.put("warp_cache_manager", warp_cache_manager);
        ret.put("skip_warp_cache_manager", skip_warp_cache_manager);
        ret.put("proxied_pages", proxied_pages);
        ret.put("proxied_time", proxied_time);
        ret.put("proxied_loaded_pages", proxied_loaded_pages);
        ret.put("proxied_loaded_pages_time", proxied_loaded_pages_time);
        ret.put("proxied_loaded_pages_bytes", proxied_loaded_pages_bytes);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        DispatcherPageSourceStats other = (DispatcherPageSourceStats) warpStatsBase;
        this.cached_files.add(other.cached_files.longValue());
        this.df_splits.add(other.df_splits.longValue());
        this.cached_warp_success_files.add(other.cached_warp_success_files.longValue());
        this.cached_warp_failed_files.add(other.cached_warp_failed_files.longValue());
        this.cached_warp_failed_pages.add(other.cached_warp_failed_pages.longValue());
        this.cached_proxied_files.add(other.cached_proxied_files.longValue());
        this.cached_total_rows.add(other.cached_total_rows.longValue());
        this.cached_read_rows.add(other.cached_read_rows.longValue());
        this.warp_match_columns.add(other.warp_match_columns.longValue());
        this.warp_match_on_simplified_domain.add(other.warp_match_on_simplified_domain.longValue());
        this.warp_collect_columns.add(other.warp_collect_columns.longValue());
        this.warp_match_collect_columns.add(other.warp_match_collect_columns.longValue());
        this.warp_mapped_match_collect_columns.add(other.warp_mapped_match_collect_columns.longValue());
        this.warp_prefilled_collect_columns.add(other.warp_prefilled_collect_columns.longValue());
        this.empty_collect_columns.add(other.empty_collect_columns.longValue());
        this.external_match_columns.add(other.external_match_columns.longValue());
        this.external_collect_columns.add(other.external_collect_columns.longValue());
        this.filtered_by_predicate.add(other.filtered_by_predicate.longValue());
        this.non_trivial_alternative_chosen.add(other.non_trivial_alternative_chosen.longValue());
        this.empty_row_group.add(other.empty_row_group.longValue());
        this.transformed_column.add(other.transformed_column.longValue());
        this.empty_page_source.add(other.empty_page_source.longValue());
        this.warp_cache_manager.add(other.warp_cache_manager.longValue());
        this.skip_warp_cache_manager.add(other.skip_warp_cache_manager.longValue());
        this.proxied_pages.add(other.proxied_pages.longValue());
        this.proxied_time.add(other.proxied_time.longValue());
        this.proxied_loaded_pages.add(other.proxied_loaded_pages.longValue());
        this.proxied_loaded_pages_time.add(other.proxied_loaded_pages_time.longValue());
        this.proxied_loaded_pages_bytes.add(other.proxied_loaded_pages_bytes.longValue());
        this.lucene_execution_time.add(other.lucene_execution_time.longValue());
        this.lucene_execution_time_Count.add(other.lucene_execution_time_Count.longValue());
        this.execution_time.add(other.execution_time.longValue());
        this.execution_time_Count.add(other.execution_time_Count.longValue());
    }

    @Override
    public void reset()
    {
        cached_files.reset();
        df_splits.reset();
        cached_warp_success_files.reset();
        cached_warp_failed_files.reset();
        cached_warp_failed_pages.reset();
        cached_proxied_files.reset();
        cached_total_rows.reset();
        cached_read_rows.reset();
        warp_match_columns.reset();
        warp_match_on_simplified_domain.reset();
        warp_collect_columns.reset();
        warp_match_collect_columns.reset();
        warp_mapped_match_collect_columns.reset();
        warp_prefilled_collect_columns.reset();
        empty_collect_columns.reset();
        external_match_columns.reset();
        external_collect_columns.reset();
        filtered_by_predicate.reset();
        non_trivial_alternative_chosen.reset();
        empty_row_group.reset();
        transformed_column.reset();
        empty_page_source.reset();
        warp_cache_manager.reset();
        skip_warp_cache_manager.reset();
        proxied_pages.reset();
        proxied_time.reset();
        proxied_loaded_pages.reset();
        proxied_loaded_pages_time.reset();
        proxied_loaded_pages_bytes.reset();
        lucene_execution_time.reset();
        lucene_execution_time_Count.reset();
        execution_time.reset();
        execution_time_Count.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":cached_files", cached_files.longValue());
        res.put(getJmxKey() + ":df_splits", df_splits.longValue());
        res.put(getJmxKey() + ":cached_warp_success_files", cached_warp_success_files.longValue());
        res.put(getJmxKey() + ":cached_warp_failed_files", cached_warp_failed_files.longValue());
        res.put(getJmxKey() + ":cached_warp_failed_pages", cached_warp_failed_pages.longValue());
        res.put(getJmxKey() + ":cached_proxied_files", cached_proxied_files.longValue());
        res.put(getJmxKey() + ":cached_total_rows", cached_total_rows.longValue());
        res.put(getJmxKey() + ":cached_read_rows", cached_read_rows.longValue());
        res.put(getJmxKey() + ":warp_match_columns", warp_match_columns.longValue());
        res.put(getJmxKey() + ":warp_match_on_simplified_domain", warp_match_on_simplified_domain.longValue());
        res.put(getJmxKey() + ":warp_collect_columns", warp_collect_columns.longValue());
        res.put(getJmxKey() + ":warp_match_collect_columns", warp_match_collect_columns.longValue());
        res.put(getJmxKey() + ":warp_mapped_match_collect_columns", warp_mapped_match_collect_columns.longValue());
        res.put(getJmxKey() + ":warp_prefilled_collect_columns", warp_prefilled_collect_columns.longValue());
        res.put(getJmxKey() + ":empty_collect_columns", empty_collect_columns.longValue());
        res.put(getJmxKey() + ":external_match_columns", external_match_columns.longValue());
        res.put(getJmxKey() + ":external_collect_columns", external_collect_columns.longValue());
        res.put(getJmxKey() + ":filtered_by_predicate", filtered_by_predicate.longValue());
        res.put(getJmxKey() + ":non_trivial_alternative_chosen", non_trivial_alternative_chosen.longValue());
        res.put(getJmxKey() + ":empty_row_group", empty_row_group.longValue());
        res.put(getJmxKey() + ":transformed_column", transformed_column.longValue());
        res.put(getJmxKey() + ":empty_page_source", empty_page_source.longValue());
        res.put(getJmxKey() + ":warp_cache_manager", warp_cache_manager.longValue());
        res.put(getJmxKey() + ":skip_warp_cache_manager", skip_warp_cache_manager.longValue());
        res.put(getJmxKey() + ":proxied_pages", proxied_pages.longValue());
        res.put(getJmxKey() + ":proxied_time", proxied_time.longValue());
        res.put(getJmxKey() + ":proxied_loaded_pages", proxied_loaded_pages.longValue());
        res.put(getJmxKey() + ":proxied_loaded_pages_time", proxied_loaded_pages_time.longValue());
        res.put(getJmxKey() + ":proxied_loaded_pages_bytes", proxied_loaded_pages_bytes.longValue());
        res.put(getJmxKey() + ":lucene_execution_time", lucene_execution_time.longValue());
        res.put(getJmxKey() + ":lucene_execution_time_Count", lucene_execution_time_Count.longValue());
        res.put(getJmxKey() + ":execution_time", execution_time.longValue());
        res.put(getJmxKey() + ":execution_time_Count", execution_time_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("cached_files", getcached_files());
        res.put("df_splits", getdf_splits());
        res.put("cached_warp_success_files", getcached_warp_success_files());
        res.put("cached_warp_failed_files", getcached_warp_failed_files());
        res.put("cached_warp_failed_pages", getcached_warp_failed_pages());
        res.put("cached_proxied_files", getcached_proxied_files());
        res.put("cached_total_rows", getcached_total_rows());
        res.put("cached_read_rows", getcached_read_rows());
        res.put("warp_match_columns", getwarp_match_columns());
        res.put("warp_match_on_simplified_domain", getwarp_match_on_simplified_domain());
        res.put("warp_collect_columns", getwarp_collect_columns());
        res.put("warp_match_collect_columns", getwarp_match_collect_columns());
        res.put("warp_mapped_match_collect_columns", getwarp_mapped_match_collect_columns());
        res.put("warp_prefilled_collect_columns", getwarp_prefilled_collect_columns());
        res.put("empty_collect_columns", getempty_collect_columns());
        res.put("external_match_columns", getexternal_match_columns());
        res.put("external_collect_columns", getexternal_collect_columns());
        res.put("filtered_by_predicate", getfiltered_by_predicate());
        res.put("non_trivial_alternative_chosen", getnon_trivial_alternative_chosen());
        res.put("empty_row_group", getempty_row_group());
        res.put("transformed_column", gettransformed_column());
        res.put("empty_page_source", getempty_page_source());
        res.put("warp_cache_manager", getwarp_cache_manager());
        res.put("skip_warp_cache_manager", getskip_warp_cache_manager());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }
}
