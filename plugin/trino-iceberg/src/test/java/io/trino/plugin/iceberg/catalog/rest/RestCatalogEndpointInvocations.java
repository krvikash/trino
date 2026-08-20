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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.testing.QueryRunner;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

final class RestCatalogEndpointInvocations
{
    private RestCatalogEndpointInvocations() {}

    public static void assertRestCatalogInvocations(QueryRunner queryRunner, Runnable runnable, Multiset<?> expectedInvocations)
    {
        IcebergConnector icebergConnector = (IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG);
        TrinoIcebergRestCatalogFactory restCatalogFactory = (TrinoIcebergRestCatalogFactory) icebergConnector.getInjector().getInstance(TrinoCatalogFactory.class);
        RestClientStats stats = restCatalogFactory.getRestClientStats();

        Map<RestCatalogEndpoint, Long> initialInvocations = new EnumMap<>(RestCatalogEndpoint.class);
        for (RestCatalogEndpoint endpoint : RestCatalogEndpoint.values()) {
            initialInvocations.put(endpoint, endpoint.getInvocationCount(stats));
        }

        runnable.run();

        ImmutableMultiset.Builder<RestCatalogEndpoint> actualInvocations = ImmutableMultiset.builder();
        for (RestCatalogEndpoint endpoint : RestCatalogEndpoint.values()) {
            long invocations = endpoint.getInvocationCount(stats) - initialInvocations.get(endpoint);
            actualInvocations.addCopies(endpoint, toIntExact(invocations));
        }

        assertThat(actualInvocations.build())
                .as("Expected invocations per endpoint")
                .isEqualTo(expectedInvocations);
    }

    public enum RestCatalogEndpoint
    {
        LOAD_TABLE(RestClientStats::getLoadTableCount),
        LOAD_VIEW(RestClientStats::getLoadViewCount);

        private final Function<RestClientStats, Long> invocationCountProvider;

        RestCatalogEndpoint(Function<RestClientStats, Long> invocationCountProvider)
        {
            this.invocationCountProvider = requireNonNull(invocationCountProvider, "invocationCountProvider is null");
        }

        public Long getInvocationCount(RestClientStats stats)
        {
            return invocationCountProvider.apply(stats);
        }
    }
}
