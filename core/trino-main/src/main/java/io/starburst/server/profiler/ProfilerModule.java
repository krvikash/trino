/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.starburst.server.profiler.rules.QueryProfilerRule;
import io.starburst.server.profiler.rules.node.ExpandingJoin;
import io.starburst.server.profiler.rules.node.FullTableScan;
import io.starburst.server.profiler.rules.node.MissingDynamicFilter;
import io.starburst.server.profiler.rules.node.MissingJdbcFilterPushdown;
import io.starburst.server.profiler.rules.node.MissingJdbcJoinPushdown;
import io.starburst.server.profiler.rules.node.MissingJdbcProjectionPushdown;
import io.starburst.server.profiler.rules.node.MissingStats;
import io.starburst.server.profiler.rules.node.SameTableScan;
import io.starburst.server.profiler.rules.node.SlowJdbcTableScan;
import io.starburst.server.profiler.rules.node.SmallFiles;
import io.starburst.server.profiler.rules.node.WrongJoinDistributionType;
import io.starburst.server.profiler.rules.node.WrongJoinOrder;
import io.starburst.server.profiler.rules.query.BigOutput;
import io.starburst.server.profiler.rules.stage.Skew;
import io.starburst.server.profiler.rules.stage.SlowSplitDistribution;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ProfilerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ProfilerConfig.class);
        configBinder(binder).bindConfig(QueryProfilerConfig.class);

        binder.bind(ProfilerListener.class).in(Scopes.SINGLETON);
        binder.bind(QueryProfiler.class).in(Scopes.SINGLETON);

        Multibinder<QueryProfilerRule> rules = Multibinder.newSetBinder(binder, QueryProfilerRule.class);
        rules.addBinding().to(SlowJdbcTableScan.class);
        rules.addBinding().to(WrongJoinOrder.class);
        rules.addBinding().to(MissingStats.class);
        rules.addBinding().to(WrongJoinDistributionType.class);
        rules.addBinding().to(SmallFiles.class);
        rules.addBinding().to(Skew.class);
        rules.addBinding().to(BigOutput.class);
        rules.addBinding().to(SameTableScan.class);
        rules.addBinding().to(FullTableScan.class);
        rules.addBinding().to(ExpandingJoin.class);
        rules.addBinding().to(SlowSplitDistribution.class);
        rules.addBinding().to(MissingJdbcJoinPushdown.class);
        rules.addBinding().to(MissingJdbcFilterPushdown.class);
        rules.addBinding().to(MissingJdbcProjectionPushdown.class);
        rules.addBinding().to(MissingDynamicFilter.class);
    }
}
