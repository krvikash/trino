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
package io.trino.sql.analyzer;

import com.google.inject.Inject;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.spi.NodeVersion;
import io.trino.sql.PlannerContext;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.sanity.ForAlternatives;
import io.trino.sql.planner.sanity.PlanSanityChecker;

import static io.trino.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static io.trino.sql.planner.sanity.PlanSanityChecker.SINGLE_NODE_PLAN_SANITY_CHECKER;
import static java.util.Objects.requireNonNull;

public class QueryExplainerFactory
{
    private final PlanOptimizersFactory planOptimizersFactory;
    private final PlanOptimizersFactory alternativesOptimizersFactory;
    private final PlanFragmenter planFragmenter;
    private final PlannerContext plannerContext;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final NodeVersion version;
    private final boolean forceSingleNodeQuery;
    private final PlanSanityChecker planSanityChecker;
    private final FormatOptions formatOptions;

    @Inject
    public QueryExplainerFactory(
            PlanOptimizersFactory planOptimizersFactory,
            @ForAlternatives PlanOptimizersFactory alternativesOptimizersFactory,
            PlanFragmenter planFragmenter,
            PlannerContext plannerContext,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            NodeVersion version,
            OptimizerConfig optimizerConfig,
            FormatOptions formatOptions)
    {
        this.planOptimizersFactory = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
        this.alternativesOptimizersFactory = requireNonNull(alternativesOptimizersFactory, "alternativesOptimizersFactory is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.plannerContext = requireNonNull(plannerContext, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.version = requireNonNull(version, "version is null");
        this.forceSingleNodeQuery = requireNonNull(optimizerConfig, "optimizerConfig is null").isForceSingleNodeQuery();
        this.planSanityChecker = forceSingleNodeQuery ? SINGLE_NODE_PLAN_SANITY_CHECKER : DISTRIBUTED_PLAN_SANITY_CHECKER;
        this.formatOptions = requireNonNull(formatOptions, "formatOptions is null");
    }

    public QueryExplainer createQueryExplainer(AnalyzerFactory analyzerFactory)
    {
        return new QueryExplainer(
                planOptimizersFactory,
                alternativesOptimizersFactory,
                planFragmenter,
                plannerContext,
                analyzerFactory,
                statsCalculator,
                costCalculator,
                version,
                forceSingleNodeQuery,
                planSanityChecker,
                formatOptions);
    }
}
