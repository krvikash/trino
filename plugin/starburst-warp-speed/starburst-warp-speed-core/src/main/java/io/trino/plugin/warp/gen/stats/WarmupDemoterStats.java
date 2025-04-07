
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
import io.trino.plugin.warp.metrics.WarpStatType;
import io.trino.plugin.warp.metrics.WarpStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName", "unused"})
public final class WarmupDemoterStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from warmupDemoter xml file for statistics and counters */
    private final LongAdder number_of_runs = new LongAdder();
    private final LongAdder number_of_runs_fail = new LongAdder();
    private final LongAdder number_of_calls = new LongAdder();
    private final LongAdder not_executed_due_threshold = new LongAdder();
    private final LongAdder not_executed_due_is_already_executing = new LongAdder();
    private final LongAdder not_executed_due_sync_demote_start_rejected = new LongAdder();
    private final LongAdder dead_objects_deleted = new LongAdder();
    private final LongAdder failed_objects_deleted = new LongAdder();
    private final LongAdder deleted_bytes = new LongAdder();
    private final LongAdder deleted_by_low_priority = new LongAdder();
    private final LongAdder number_of_cycles = new LongAdder();
    private final LongAdder number_fail_acquire = new LongAdder();
    private final LongAdder currentUsage = new LongAdder();
    private final LongAdder totalUsage = new LongAdder();
    private final LongAdder reserved_tx = new LongAdder();
    private final LongAdder failed_row_group_data = new LongAdder();
    private final LongAdder waiting_for_lock_nano_Count = new LongAdder();
    private final LongAdder waiting_for_lock_nano = new LongAdder();
    private final LongAdder execution_time_nano_Count = new LongAdder();
    private final LongAdder execution_time_nano = new LongAdder();

    @JsonCreator
    public WarmupDemoterStats()
    {
        super(createKey(), WarpStatType.Worker);
    }

    @JsonIgnore
    @Managed
    public long getnumber_of_runs()
    {
        return number_of_runs.longValue();
    }

    public void incnumber_of_runs()
    {
        number_of_runs.increment();
    }

    public void addnumber_of_runs(long val)
    {
        number_of_runs.add(val);
    }

    public void setnumber_of_runs(long val)
    {
        number_of_runs.reset();
        addnumber_of_runs(val);
    }

    @JsonIgnore
    @Managed
    public long getnumber_of_runs_fail()
    {
        return number_of_runs_fail.longValue();
    }

    public void incnumber_of_runs_fail()
    {
        number_of_runs_fail.increment();
    }

    public void addnumber_of_runs_fail(long val)
    {
        number_of_runs_fail.add(val);
    }

    public void setnumber_of_runs_fail(long val)
    {
        number_of_runs_fail.reset();
        addnumber_of_runs_fail(val);
    }

    @JsonIgnore
    @Managed
    public long getnumber_of_calls()
    {
        return number_of_calls.longValue();
    }

    public void incnumber_of_calls()
    {
        number_of_calls.increment();
    }

    public void addnumber_of_calls(long val)
    {
        number_of_calls.add(val);
    }

    public void setnumber_of_calls(long val)
    {
        number_of_calls.reset();
        addnumber_of_calls(val);
    }

    @JsonIgnore
    @Managed
    public long getnot_executed_due_threshold()
    {
        return not_executed_due_threshold.longValue();
    }

    public void incnot_executed_due_threshold()
    {
        not_executed_due_threshold.increment();
    }

    public void addnot_executed_due_threshold(long val)
    {
        not_executed_due_threshold.add(val);
    }

    public void setnot_executed_due_threshold(long val)
    {
        not_executed_due_threshold.reset();
        addnot_executed_due_threshold(val);
    }

    @JsonIgnore
    @Managed
    public long getnot_executed_due_is_already_executing()
    {
        return not_executed_due_is_already_executing.longValue();
    }

    public void incnot_executed_due_is_already_executing()
    {
        not_executed_due_is_already_executing.increment();
    }

    public void addnot_executed_due_is_already_executing(long val)
    {
        not_executed_due_is_already_executing.add(val);
    }

    public void setnot_executed_due_is_already_executing(long val)
    {
        not_executed_due_is_already_executing.reset();
        addnot_executed_due_is_already_executing(val);
    }

    @JsonIgnore
    @Managed
    public long getnot_executed_due_sync_demote_start_rejected()
    {
        return not_executed_due_sync_demote_start_rejected.longValue();
    }

    public void incnot_executed_due_sync_demote_start_rejected()
    {
        not_executed_due_sync_demote_start_rejected.increment();
    }

    public void addnot_executed_due_sync_demote_start_rejected(long val)
    {
        not_executed_due_sync_demote_start_rejected.add(val);
    }

    public void setnot_executed_due_sync_demote_start_rejected(long val)
    {
        not_executed_due_sync_demote_start_rejected.reset();
        addnot_executed_due_sync_demote_start_rejected(val);
    }

    @JsonIgnore
    @Managed
    public long getdead_objects_deleted()
    {
        return dead_objects_deleted.longValue();
    }

    public void incdead_objects_deleted()
    {
        dead_objects_deleted.increment();
    }

    public void adddead_objects_deleted(long val)
    {
        dead_objects_deleted.add(val);
    }

    public void setdead_objects_deleted(long val)
    {
        dead_objects_deleted.reset();
        adddead_objects_deleted(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_objects_deleted()
    {
        return failed_objects_deleted.longValue();
    }

    public void incfailed_objects_deleted()
    {
        failed_objects_deleted.increment();
    }

    public void addfailed_objects_deleted(long val)
    {
        failed_objects_deleted.add(val);
    }

    public void setfailed_objects_deleted(long val)
    {
        failed_objects_deleted.reset();
        addfailed_objects_deleted(val);
    }

    @JsonIgnore
    @Managed
    public long getdeleted_bytes()
    {
        return deleted_bytes.longValue();
    }

    public void incdeleted_bytes()
    {
        deleted_bytes.increment();
    }

    public void adddeleted_bytes(long val)
    {
        deleted_bytes.add(val);
    }

    public void setdeleted_bytes(long val)
    {
        deleted_bytes.reset();
        adddeleted_bytes(val);
    }

    @JsonIgnore
    @Managed
    public long getdeleted_by_low_priority()
    {
        return deleted_by_low_priority.longValue();
    }

    public void incdeleted_by_low_priority()
    {
        deleted_by_low_priority.increment();
    }

    public void adddeleted_by_low_priority(long val)
    {
        deleted_by_low_priority.add(val);
    }

    public void setdeleted_by_low_priority(long val)
    {
        deleted_by_low_priority.reset();
        adddeleted_by_low_priority(val);
    }

    @JsonIgnore
    @Managed
    public long getnumber_of_cycles()
    {
        return number_of_cycles.longValue();
    }

    public void incnumber_of_cycles()
    {
        number_of_cycles.increment();
    }

    public void addnumber_of_cycles(long val)
    {
        number_of_cycles.add(val);
    }

    public void setnumber_of_cycles(long val)
    {
        number_of_cycles.reset();
        addnumber_of_cycles(val);
    }

    @JsonIgnore
    @Managed
    public long getnumber_fail_acquire()
    {
        return number_fail_acquire.longValue();
    }

    public void incnumber_fail_acquire()
    {
        number_fail_acquire.increment();
    }

    public void addnumber_fail_acquire(long val)
    {
        number_fail_acquire.add(val);
    }

    public void setnumber_fail_acquire(long val)
    {
        number_fail_acquire.reset();
        addnumber_fail_acquire(val);
    }

    @JsonIgnore
    @Managed
    public long getcurrentUsage()
    {
        return currentUsage.longValue();
    }

    public void inccurrentUsage()
    {
        currentUsage.increment();
    }

    public void addcurrentUsage(long val)
    {
        currentUsage.add(val);
    }

    public void setcurrentUsage(long val)
    {
        currentUsage.reset();
        addcurrentUsage(val);
    }

    @JsonIgnore
    @Managed
    public long gettotalUsage()
    {
        return totalUsage.longValue();
    }

    public void inctotalUsage()
    {
        totalUsage.increment();
    }

    public void addtotalUsage(long val)
    {
        totalUsage.add(val);
    }

    public void settotalUsage(long val)
    {
        totalUsage.reset();
        addtotalUsage(val);
    }

    @JsonIgnore
    @Managed
    public long getreserved_tx()
    {
        return reserved_tx.longValue();
    }

    public void increserved_tx()
    {
        reserved_tx.increment();
    }

    public void addreserved_tx(long val)
    {
        reserved_tx.add(val);
    }

    public void setreserved_tx(long val)
    {
        reserved_tx.reset();
        addreserved_tx(val);
    }

    @JsonIgnore
    @Managed
    public long getfailed_row_group_data()
    {
        return failed_row_group_data.longValue();
    }

    public void incfailed_row_group_data()
    {
        failed_row_group_data.increment();
    }

    public void addfailed_row_group_data(long val)
    {
        failed_row_group_data.add(val);
    }

    public void setfailed_row_group_data(long val)
    {
        failed_row_group_data.reset();
        addfailed_row_group_data(val);
    }

    @JsonIgnore
    @Managed
    public long getwaiting_for_lock_nano_Count()
    {
        return waiting_for_lock_nano_Count.longValue();
    }

    @Managed
    public long getwaiting_for_lock_nano_Average()
    {
        if (waiting_for_lock_nano_Count.longValue() == 0) {
            return 0;
        }
        return waiting_for_lock_nano.longValue() / waiting_for_lock_nano_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getwaiting_for_lock_nano()
    {
        return waiting_for_lock_nano.longValue();
    }

    public void addwaiting_for_lock_nano(long val)
    {
        waiting_for_lock_nano.add(val);
        waiting_for_lock_nano_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getexecution_time_nano_Count()
    {
        return execution_time_nano_Count.longValue();
    }

    @Managed
    public long getexecution_time_nano_Average()
    {
        if (execution_time_nano_Count.longValue() == 0) {
            return 0;
        }
        return execution_time_nano.longValue() / execution_time_nano_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getexecution_time_nano()
    {
        return execution_time_nano.longValue();
    }

    public void addexecution_time_nano(long val)
    {
        execution_time_nano.add(val);
        execution_time_nano_Count.add(1);
    }

    public static WarmupDemoterStats create()
    {
        return new WarmupDemoterStats();
    }

    public static String createKey()
    {
        return "warmupDemoter";
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("number_of_runs", number_of_runs);
        ret.put("number_of_runs_fail", number_of_runs_fail);
        ret.put("number_of_calls", number_of_calls);
        ret.put("not_executed_due_threshold", not_executed_due_threshold);
        ret.put("not_executed_due_is_already_executing", not_executed_due_is_already_executing);
        ret.put("not_executed_due_sync_demote_start_rejected", not_executed_due_sync_demote_start_rejected);
        ret.put("dead_objects_deleted", dead_objects_deleted);
        ret.put("failed_objects_deleted", failed_objects_deleted);
        ret.put("deleted_bytes", deleted_bytes);
        ret.put("deleted_by_low_priority", deleted_by_low_priority);
        ret.put("number_of_cycles", number_of_cycles);
        ret.put("number_fail_acquire", number_fail_acquire);
        ret.put("currentUsage", currentUsage);
        ret.put("totalUsage", totalUsage);
        ret.put("reserved_tx", reserved_tx);
        ret.put("failed_row_group_data", failed_row_group_data);

        return ret;
    }

    @Override
    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        if (warpStatsBase == null) {
            return;
        }
        WarmupDemoterStats other = (WarmupDemoterStats) warpStatsBase;
        this.number_of_runs.add(other.number_of_runs.longValue());
        this.number_of_runs_fail.add(other.number_of_runs_fail.longValue());
        this.number_of_calls.add(other.number_of_calls.longValue());
        this.not_executed_due_threshold.add(other.not_executed_due_threshold.longValue());
        this.not_executed_due_is_already_executing.add(other.not_executed_due_is_already_executing.longValue());
        this.not_executed_due_sync_demote_start_rejected.add(other.not_executed_due_sync_demote_start_rejected.longValue());
        this.dead_objects_deleted.add(other.dead_objects_deleted.longValue());
        this.failed_objects_deleted.add(other.failed_objects_deleted.longValue());
        this.deleted_bytes.add(other.deleted_bytes.longValue());
        this.deleted_by_low_priority.add(other.deleted_by_low_priority.longValue());
        this.number_of_cycles.add(other.number_of_cycles.longValue());
        this.number_fail_acquire.add(other.number_fail_acquire.longValue());
        this.currentUsage.add(other.currentUsage.longValue());
        this.totalUsage.add(other.totalUsage.longValue());
        this.reserved_tx.add(other.reserved_tx.longValue());
        this.failed_row_group_data.add(other.failed_row_group_data.longValue());
        this.waiting_for_lock_nano.add(other.waiting_for_lock_nano.longValue());
        this.waiting_for_lock_nano_Count.add(other.waiting_for_lock_nano_Count.longValue());
        this.execution_time_nano.add(other.execution_time_nano.longValue());
        this.execution_time_nano_Count.add(other.execution_time_nano_Count.longValue());
    }

    @Override
    public void reset()
    {
        number_of_runs.reset();
        number_of_runs_fail.reset();
        number_of_calls.reset();
        not_executed_due_threshold.reset();
        not_executed_due_is_already_executing.reset();
        not_executed_due_sync_demote_start_rejected.reset();
        dead_objects_deleted.reset();
        failed_objects_deleted.reset();
        deleted_bytes.reset();
        deleted_by_low_priority.reset();
        number_of_cycles.reset();
        number_fail_acquire.reset();
        currentUsage.reset();
        totalUsage.reset();
        reserved_tx.reset();
        failed_row_group_data.reset();
        waiting_for_lock_nano.reset();
        waiting_for_lock_nano_Count.reset();
        execution_time_nano.reset();
        execution_time_nano_Count.reset();
    }

    @Override
    public Map<String, Long> statsCounterMapper()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("warmupDemoter:number_of_runs", number_of_runs.longValue());
        res.put("warmupDemoter:number_of_runs_fail", number_of_runs_fail.longValue());
        res.put("warmupDemoter:number_of_calls", number_of_calls.longValue());
        res.put("warmupDemoter:not_executed_due_threshold", not_executed_due_threshold.longValue());
        res.put("warmupDemoter:not_executed_due_is_already_executing", not_executed_due_is_already_executing.longValue());
        res.put("warmupDemoter:not_executed_due_sync_demote_start_rejected", not_executed_due_sync_demote_start_rejected.longValue());
        res.put("warmupDemoter:dead_objects_deleted", dead_objects_deleted.longValue());
        res.put("warmupDemoter:failed_objects_deleted", failed_objects_deleted.longValue());
        res.put("warmupDemoter:deleted_bytes", deleted_bytes.longValue());
        res.put("warmupDemoter:deleted_by_low_priority", deleted_by_low_priority.longValue());
        res.put("warmupDemoter:number_of_cycles", number_of_cycles.longValue());
        res.put("warmupDemoter:number_fail_acquire", number_fail_acquire.longValue());
        res.put("warmupDemoter:currentUsage", currentUsage.longValue());
        res.put("warmupDemoter:totalUsage", totalUsage.longValue());
        res.put("warmupDemoter:reserved_tx", reserved_tx.longValue());
        res.put("warmupDemoter:failed_row_group_data", failed_row_group_data.longValue());
        res.put("warmupDemoter:waiting_for_lock_nano", waiting_for_lock_nano.longValue());
        res.put("warmupDemoter:waiting_for_lock_nano_Count", waiting_for_lock_nano_Count.longValue());
        res.put("warmupDemoter:execution_time_nano", execution_time_nano.longValue());
        res.put("warmupDemoter:execution_time_nano_Count", execution_time_nano_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("number_of_runs", getnumber_of_runs());
        res.put("number_of_runs_fail", getnumber_of_runs_fail());
        res.put("number_of_calls", getnumber_of_calls());
        res.put("not_executed_due_threshold", getnot_executed_due_threshold());
        res.put("not_executed_due_is_already_executing", getnot_executed_due_is_already_executing());
        res.put("not_executed_due_sync_demote_start_rejected", getnot_executed_due_sync_demote_start_rejected());
        res.put("dead_objects_deleted", getdead_objects_deleted());
        res.put("failed_objects_deleted", getfailed_objects_deleted());
        res.put("deleted_bytes", getdeleted_bytes());
        res.put("deleted_by_low_priority", getdeleted_by_low_priority());
        res.put("number_of_cycles", getnumber_of_cycles());
        res.put("number_fail_acquire", getnumber_fail_acquire());
        res.put("currentUsage", getcurrentUsage());
        res.put("reserved_tx", getreserved_tx());
        res.put("failed_row_group_data", getfailed_row_group_data());
        return res;
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("waiting_for_lock_nano", getwaiting_for_lock_nano());
        res.put("waiting_for_lock_nano_Count", getwaiting_for_lock_nano_Count());
        res.put("execution_time_nano", getexecution_time_nano());
        res.put("execution_time_nano_Count", getexecution_time_nano_Count());
        return res;
    }
}
