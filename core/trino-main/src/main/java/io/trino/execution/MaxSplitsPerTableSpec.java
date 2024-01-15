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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.metadata.QualifiedObjectName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public record MaxSplitsPerTableSpec(@JsonProperty("limits") List<MaxSplitsPerTableRule> limits)
{
    public MaxSplitsPerTableSpec(List<MaxSplitsPerTableRule> limits)
    {
        this.limits = ImmutableList.copyOf(limits);
    }

    public record MaxSplitsPerTableRule(@JsonProperty("table") String table, @JsonProperty("limit") long limit) {}

    public static class MaxSplitsPerTableSpecProvider
    {
        private static final Logger log = Logger.get(QueryTracker.class);

        private long maxAllowedSplitCountPerTableLastUpdate;
        private Map<QualifiedObjectName, Long> limits;
        private final Ticker ticker;
        private final Duration refreshPeriod;
        private final String configFilePath;

        @Inject
        public MaxSplitsPerTableSpecProvider(Ticker ticker, MaxSplitsPerTableConfig config)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            requireNonNull(config, "config is null");
            this.refreshPeriod = requireNonNull(config.getRefreshPeriod(), "period is null");
            this.configFilePath = config.getQueryMaxSplitsPerTableConfigFilePath();
            this.limits = getMaxAllowedSplitCountPerTable(true);
        }

        public synchronized Map<QualifiedObjectName, Long> getMaxAllowedSplitCountPerTable()
        {
            return getMaxAllowedSplitCountPerTable(false);
        }

        private synchronized Map<QualifiedObjectName, Long> getMaxAllowedSplitCountPerTable(boolean failOnInvalidSpecification)
        {
            if (ticker.read() - maxAllowedSplitCountPerTableLastUpdate >= MILLISECONDS.toNanos(refreshPeriod.toMillis()) || limits == null) {
                limits = parseMaxSplitsPerTableConfig(failOnInvalidSpecification);
                maxAllowedSplitCountPerTableLastUpdate = ticker.read();
            }
            return limits;
        }

        private Map<QualifiedObjectName, Long> parseMaxSplitsPerTableConfig(boolean failOnInvalidSpecification)
        {
            String queryMaxSplitsPerTableConfigFilePath = configFilePath;
            if (queryMaxSplitsPerTableConfigFilePath == null) {
                return ImmutableMap.of();
            }
            try {
                return parseJson(Files.readAllBytes(Path.of(queryMaxSplitsPerTableConfigFilePath)), MaxSplitsPerTableSpec.class)
                        .limits()
                        .stream()
                        .collect(toImmutableMap(rule -> QualifiedObjectName.valueOf(rule.table()), MaxSplitsPerTableRule::limit));
            }
            catch (Exception e) {
                log.warn(e, "Cannot load file %s. Error is: %s".formatted(configFilePath, e.getMessage()));
                if (failOnInvalidSpecification) {
                    throw new IllegalArgumentException(e.getMessage(), e);
                }
            }
            return limits;
        }
    }
}
