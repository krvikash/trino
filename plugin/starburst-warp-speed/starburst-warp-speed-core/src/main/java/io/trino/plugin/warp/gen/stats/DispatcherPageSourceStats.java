
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
public final class DispatcherPageSourceStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from dispatcherPageSource xml file for statistics and counters */
    private final LongAdder cached_files = new LongAdder();
    private final LongAdder df_splits = new LongAdder();
    private final LongAdder cached_warp_success_files = new LongAdder();
    private final LongAdder cached_warp_failed_files = new LongAdder();
    private final LongAdder cached_warp_failed_pages = new LongAdder();
    private final LongAdder cached_proxied_files = new LongAdder();
    private final LongAdder inefficient_filtering = new LongAdder();
    private final LongAdder efficient_filtering = new LongAdder();
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
    private final LongAdder proxied_pages = new LongAdder();
    private final LongAdder proxied_time = new LongAdder();
    private final LongAdder proxied_loaded_pages = new LongAdder();
    private final LongAdder proxied_loaded_pages_time = new LongAdder();
    private final LongAdder proxied_loaded_pages_bytes = new LongAdder();
    private final LongAdder lazy_collect_total_blocks = new LongAdder();
    private final LongAdder lazy_collect_loaded_blocks = new LongAdder();
    private final LongAdder lazy_collect_failed_load = new LongAdder();
    private final LongAdder native_read_time = new LongAdder();
    private final LongAdder block_fillers_time = new LongAdder();
    private final LongAdder lucene_execution_time_Count = new LongAdder();
    private final LongAdder lucene_execution_time = new LongAdder();
    private final LongAdder execution_time_Count = new LongAdder();
    private final LongAdder execution_time = new LongAdder();

    @JsonCreator
    public DispatcherPageSourceStats()
    {
        super(createKey());
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
    public long getinefficient_filtering()
    {
        return inefficient_filtering.longValue();
    }

    public void incinefficient_filtering()
    {
        inefficient_filtering.increment();
    }

    public void addinefficient_filtering(long val)
    {
        inefficient_filtering.add(val);
    }

    public void setinefficient_filtering(long val)
    {
        inefficient_filtering.reset();
        addinefficient_filtering(val);
    }

    @JsonIgnore
    @Managed
    public long getefficient_filtering()
    {
        return efficient_filtering.longValue();
    }

    public void incefficient_filtering()
    {
        efficient_filtering.increment();
    }

    public void addefficient_filtering(long val)
    {
        efficient_filtering.add(val);
    }

    public void setefficient_filtering(long val)
    {
        efficient_filtering.reset();
        addefficient_filtering(val);
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
    public long getlazy_collect_total_blocks()
    {
        return lazy_collect_total_blocks.longValue();
    }

    public void inclazy_collect_total_blocks()
    {
        lazy_collect_total_blocks.increment();
    }

    public void addlazy_collect_total_blocks(long val)
    {
        lazy_collect_total_blocks.add(val);
    }

    public void setlazy_collect_total_blocks(long val)
    {
        lazy_collect_total_blocks.reset();
        addlazy_collect_total_blocks(val);
    }

    @JsonIgnore
    @Managed
    public long getlazy_collect_loaded_blocks()
    {
        return lazy_collect_loaded_blocks.longValue();
    }

    public void inclazy_collect_loaded_blocks()
    {
        lazy_collect_loaded_blocks.increment();
    }

    public void addlazy_collect_loaded_blocks(long val)
    {
        lazy_collect_loaded_blocks.add(val);
    }

    public void setlazy_collect_loaded_blocks(long val)
    {
        lazy_collect_loaded_blocks.reset();
        addlazy_collect_loaded_blocks(val);
    }

    @JsonIgnore
    @Managed
    public long getlazy_collect_failed_load()
    {
        return lazy_collect_failed_load.longValue();
    }

    public void inclazy_collect_failed_load()
    {
        lazy_collect_failed_load.increment();
    }

    public void addlazy_collect_failed_load(long val)
    {
        lazy_collect_failed_load.add(val);
    }

    public void setlazy_collect_failed_load(long val)
    {
        lazy_collect_failed_load.reset();
        addlazy_collect_failed_load(val);
    }

    @JsonIgnore
    @Managed
    public long getnative_read_time()
    {
        return native_read_time.longValue();
    }

    public void incnative_read_time()
    {
        native_read_time.increment();
    }

    public void addnative_read_time(long val)
    {
        native_read_time.add(val);
    }

    public void setnative_read_time(long val)
    {
        native_read_time.reset();
        addnative_read_time(val);
    }

    @JsonIgnore
    @Managed
    public long getblock_fillers_time()
    {
        return block_fillers_time.longValue();
    }

    public void incblock_fillers_time()
    {
        block_fillers_time.increment();
    }

    public void addblock_fillers_time(long val)
    {
        block_fillers_time.add(val);
    }

    public void setblock_fillers_time(long val)
    {
        block_fillers_time.reset();
        addblock_fillers_time(val);
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

    public static DispatcherPageSourceStats create()
    {
        return new DispatcherPageSourceStats();
    }

    public static String createKey()
    {
        return "dispatcherPageSource";
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
        ret.put("inefficient_filtering", inefficient_filtering);
        ret.put("efficient_filtering", efficient_filtering);
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
        ret.put("proxied_pages", proxied_pages);
        ret.put("proxied_time", proxied_time);
        ret.put("proxied_loaded_pages", proxied_loaded_pages);
        ret.put("proxied_loaded_pages_time", proxied_loaded_pages_time);
        ret.put("proxied_loaded_pages_bytes", proxied_loaded_pages_bytes);
        ret.put("lazy_collect_total_blocks", lazy_collect_total_blocks);
        ret.put("lazy_collect_loaded_blocks", lazy_collect_loaded_blocks);
        ret.put("lazy_collect_failed_load", lazy_collect_failed_load);
        ret.put("native_read_time", native_read_time);
        ret.put("block_fillers_time", block_fillers_time);

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
        this.inefficient_filtering.add(other.inefficient_filtering.longValue());
        this.efficient_filtering.add(other.efficient_filtering.longValue());
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
        this.proxied_pages.add(other.proxied_pages.longValue());
        this.proxied_time.add(other.proxied_time.longValue());
        this.proxied_loaded_pages.add(other.proxied_loaded_pages.longValue());
        this.proxied_loaded_pages_time.add(other.proxied_loaded_pages_time.longValue());
        this.proxied_loaded_pages_bytes.add(other.proxied_loaded_pages_bytes.longValue());
        this.lazy_collect_total_blocks.add(other.lazy_collect_total_blocks.longValue());
        this.lazy_collect_loaded_blocks.add(other.lazy_collect_loaded_blocks.longValue());
        this.lazy_collect_failed_load.add(other.lazy_collect_failed_load.longValue());
        this.native_read_time.add(other.native_read_time.longValue());
        this.block_fillers_time.add(other.block_fillers_time.longValue());
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
        inefficient_filtering.reset();
        efficient_filtering.reset();
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
        proxied_pages.reset();
        proxied_time.reset();
        proxied_loaded_pages.reset();
        proxied_loaded_pages_time.reset();
        proxied_loaded_pages_bytes.reset();
        lazy_collect_total_blocks.reset();
        lazy_collect_loaded_blocks.reset();
        lazy_collect_failed_load.reset();
        native_read_time.reset();
        block_fillers_time.reset();
        lucene_execution_time.reset();
        lucene_execution_time_Count.reset();
        execution_time.reset();
        execution_time_Count.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        if (cached_files.longValue() > 0) {
            res.put("dispatcherPageSource:cached_files", cached_files.longValue());
        }
        if (df_splits.longValue() > 0) {
            res.put("dispatcherPageSource:df_splits", df_splits.longValue());
        }
        res.put("dispatcherPageSource:cached_warp_success_files", cached_warp_success_files.longValue());
        res.put("dispatcherPageSource:cached_warp_failed_files", cached_warp_failed_files.longValue());
        res.put("dispatcherPageSource:cached_warp_failed_pages", cached_warp_failed_pages.longValue());
        res.put("dispatcherPageSource:cached_proxied_files", cached_proxied_files.longValue());
        res.put("dispatcherPageSource:inefficient_filtering", inefficient_filtering.longValue());
        res.put("dispatcherPageSource:efficient_filtering", efficient_filtering.longValue());
        if (cached_total_rows.longValue() > 0) {
            res.put("dispatcherPageSource:cached_total_rows", cached_total_rows.longValue());
        }
        if (cached_read_rows.longValue() > 0) {
            res.put("dispatcherPageSource:cached_read_rows", cached_read_rows.longValue());
        }
        res.put("dispatcherPageSource:warp_match_columns", warp_match_columns.longValue());
        res.put("dispatcherPageSource:warp_match_on_simplified_domain", warp_match_on_simplified_domain.longValue());
        res.put("dispatcherPageSource:warp_collect_columns", warp_collect_columns.longValue());
        res.put("dispatcherPageSource:warp_match_collect_columns", warp_match_collect_columns.longValue());
        res.put("dispatcherPageSource:warp_mapped_match_collect_columns", warp_mapped_match_collect_columns.longValue());
        res.put("dispatcherPageSource:warp_prefilled_collect_columns", warp_prefilled_collect_columns.longValue());
        res.put("dispatcherPageSource:empty_collect_columns", empty_collect_columns.longValue());
        res.put("dispatcherPageSource:external_match_columns", external_match_columns.longValue());
        res.put("dispatcherPageSource:external_collect_columns", external_collect_columns.longValue());
        res.put("dispatcherPageSource:filtered_by_predicate", filtered_by_predicate.longValue());
        res.put("dispatcherPageSource:non_trivial_alternative_chosen", non_trivial_alternative_chosen.longValue());
        if (empty_row_group.longValue() > 0) {
            res.put("dispatcherPageSource:empty_row_group", empty_row_group.longValue());
        }
        if (transformed_column.longValue() > 0) {
            res.put("dispatcherPageSource:transformed_column", transformed_column.longValue());
        }
        if (empty_page_source.longValue() > 0) {
            res.put("dispatcherPageSource:empty_page_source", empty_page_source.longValue());
        }
        if (proxied_pages.longValue() > 0) {
            res.put("dispatcherPageSource:proxied_pages", proxied_pages.longValue());
        }
        if (proxied_time.longValue() > 0) {
            res.put("dispatcherPageSource:proxied_time", proxied_time.longValue());
        }
        if (proxied_loaded_pages.longValue() > 0) {
            res.put("dispatcherPageSource:proxied_loaded_pages", proxied_loaded_pages.longValue());
        }
        if (proxied_loaded_pages_time.longValue() > 0) {
            res.put("dispatcherPageSource:proxied_loaded_pages_time", proxied_loaded_pages_time.longValue());
        }
        if (proxied_loaded_pages_bytes.longValue() > 0) {
            res.put("dispatcherPageSource:proxied_loaded_pages_bytes", proxied_loaded_pages_bytes.longValue());
        }
        res.put("dispatcherPageSource:lazy_collect_total_blocks", lazy_collect_total_blocks.longValue());
        res.put("dispatcherPageSource:lazy_collect_loaded_blocks", lazy_collect_loaded_blocks.longValue());
        if (native_read_time.longValue() > 0) {
            res.put("dispatcherPageSource:native_read_time", native_read_time.longValue());
        }
        if (block_fillers_time.longValue() > 0) {
            res.put("dispatcherPageSource:block_fillers_time", block_fillers_time.longValue());
        }
        if (lucene_execution_time.longValue() > 0) {
            res.put("dispatcherPageSource:lucene_execution_time", lucene_execution_time.longValue());
        }
        if (lucene_execution_time.longValue() > 0) {
            res.put("dispatcherPageSource:lucene_execution_time_Count", lucene_execution_time_Count.longValue());
        }
        if (execution_time.longValue() > 0) {
            res.put("dispatcherPageSource:execution_time", execution_time.longValue());
        }
        if (execution_time.longValue() > 0) {
            res.put("dispatcherPageSource:execution_time_Count", execution_time_Count.longValue());
        }
        return res;
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("cached_files", getcached_files());
        res.put("df_splits", getdf_splits());
        res.put("cached_warp_success_files", getcached_warp_success_files());
        res.put("cached_warp_failed_files", getcached_warp_failed_files());
        res.put("cached_warp_failed_pages", getcached_warp_failed_pages());
        res.put("cached_proxied_files", getcached_proxied_files());
        res.put("inefficient_filtering", getinefficient_filtering());
        res.put("efficient_filtering", getefficient_filtering());
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
        res.put("lazy_collect_total_blocks", getlazy_collect_total_blocks());
        res.put("lazy_collect_loaded_blocks", getlazy_collect_loaded_blocks());
        res.put("lazy_collect_failed_load", getlazy_collect_failed_load());
        return res;
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        return new HashMap<>();
    }
}
