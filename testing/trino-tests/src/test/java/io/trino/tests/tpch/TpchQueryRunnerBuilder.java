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
package io.trino.tests.tpch;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.connector.alternatives.MockPlanAlternativePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TpchQueryRunnerBuilder
        extends DistributedQueryRunner.Builder<TpchQueryRunnerBuilder>
{
    private Map<String, String> connectorProperties = ImmutableMap.of();
    private boolean withPlanAlternatives;

    private TpchQueryRunnerBuilder()
    {
        super(testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build());
    }

    @CanIgnoreReturnValue
    public TpchQueryRunnerBuilder withConnectorProperties(Map<String, String> connectorProperties)
    {
        this.connectorProperties = ImmutableMap.copyOf(connectorProperties);
        return this;
    }

    public TpchQueryRunnerBuilder withPlanAlternatives()
    {
        this.withPlanAlternatives = true;
        return this;
    }

    public static TpchQueryRunnerBuilder builder()
    {
        return new TpchQueryRunnerBuilder();
    }

    @Override
    public DistributedQueryRunner build()
            throws Exception
    {
        if (withPlanAlternatives) {
            super.addExtraProperty("optimizer.use-sub-plan-alternatives", "true");
        }
        DistributedQueryRunner queryRunner = super.build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new MockPlanAlternativePlugin(new TpchPlugin()));
            queryRunner.createCatalog("tpch", withPlanAlternatives ? "plan_alternatives_tpch" : "tpch", connectorProperties);
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
