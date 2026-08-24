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
package io.trino.plugin.prometheus.substitution;

import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.plugin.prometheus.PrometheusQueryRunner;
import io.trino.plugin.prometheus.PrometheusServer;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;

/**
 * MV substitution over a Prometheus source. Prometheus is read-only and fixed-schema: its tables are
 * the Prometheus server's metrics, each exposing the same {@code labels}/{@code timestamp}/{@code value}
 * columns, so it cannot host the arbitrary source tables {@link io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest}
 * creates. It uses the read-only suite over always-present self-scraped metrics instead.
 * <p>
 * {@code timestamp} is left out of {@link #substitutedColumns()} because Iceberg MV storage does not
 * round-trip {@code timestamp(3) with time zone} (it stores it as {@code varchar}); {@code labels} and
 * {@code value} map cleanly.
 */
public class TestIcebergMvOnPrometheusSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        PrometheusServer server = closeAfterClass(new PrometheusServer());
        return PrometheusQueryRunner.builder(server)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        // The `up` metric is scraped by the Prometheus server on itself and is always present.
        return new CatalogSchemaTableName("prometheus", "default", "up");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        // Another always-present self-scraped metric, distinct from `up`.
        return new CatalogSchemaTableName("prometheus", "default", "go_goroutines");
    }

    @Override
    protected String materializedColumn()
    {
        return "value";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "labels";
    }
}
