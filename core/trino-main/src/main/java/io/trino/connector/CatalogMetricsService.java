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
package io.trino.connector;

import com.google.inject.Inject;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class CatalogMetricsService
{
    private static final String METER_INSTRUMENTATION_NAME = "com.starburstdata.presto.telemetry.dynamic_catalogs";
    private static final String METER_INSTRUMENTATION_VERSION = "1.0.0";
    public static final AttributeKey<String> ATTRIBUTE_KEY = AttributeKey.stringKey("connector");
    public static final String UNIT_VALUE = "1";

    private final AtomicLong catalogTotal = new AtomicLong(0);
    private final LongGauge catalogTotalGauge;
    private final LongCounter catalogsCreated;
    private final LongCounter catalogsRenamed;
    private final LongCounter catalogsAltered;
    private final LongCounter catalogsDropped;

    @Inject
    public CatalogMetricsService(Optional<MeterProvider> meterProvider)
    {
        Meter meter = meterProvider.orElseThrow(() -> new IllegalStateException("meterProvider is empty"))
                .meterBuilder(METER_INSTRUMENTATION_NAME)
                .setInstrumentationVersion(METER_INSTRUMENTATION_VERSION)
                .build();
        this.catalogTotalGauge = meter.gaugeBuilder("catalogs_total").setUnit(UNIT_VALUE).ofLongs().build();
        this.catalogsCreated = meter.counterBuilder("catalogs_created").setUnit(UNIT_VALUE).build();
        this.catalogsRenamed = meter.counterBuilder("catalogs_renamed").setUnit(UNIT_VALUE).build();
        this.catalogsAltered = meter.counterBuilder("catalogs_altered").setUnit(UNIT_VALUE).build();
        this.catalogsDropped = meter.counterBuilder("catalogs_dropped").setUnit(UNIT_VALUE).build();
    }

    public void addCatalog()
    {
        long total = catalogTotal.incrementAndGet();
        catalogTotalGauge.set(total);
    }

    public void catalogCreated(String connectorName)
    {
        long catalogTotal = this.catalogTotal.incrementAndGet();
        catalogTotalGauge.set(catalogTotal, Attributes.of(ATTRIBUTE_KEY, connectorName));
        catalogsCreated.add(1, Attributes.of(ATTRIBUTE_KEY, connectorName));
    }

    public void catalogRenamed(String connectorName)
    {
        catalogsRenamed.add(1, Attributes.of(ATTRIBUTE_KEY, connectorName));
    }

    public void catalogAltered(String connectorName)
    {
        catalogsAltered.add(1, Attributes.of(ATTRIBUTE_KEY, connectorName));
    }

    public void catalogDropped(String connectorName)
    {
        long catalogTotal = this.catalogTotal.decrementAndGet();
        catalogTotalGauge.set(catalogTotal, Attributes.of(ATTRIBUTE_KEY, connectorName));
        catalogsDropped.add(1, Attributes.of(ATTRIBUTE_KEY, connectorName));
    }
}
