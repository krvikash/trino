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
package io.trino.plugin.warp;

import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherStatisticsProvider;
import io.trino.spi.statistics.Estimate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DispatcherStatisticsProviderTest
{
    private final DispatcherProxiedConnectorTransformer transformer = new TestingConnectorProxiedConnectorTransformer();

    @Test
    public void cardinalityIllegalConfigShouldThrow()
    {
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setCardinalityBuckets("");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(transformer, globalConfig));
        globalConfig.setCardinalityBuckets("1.5");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(transformer, globalConfig));
        globalConfig.setCardinalityBuckets("abc");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(transformer, globalConfig));
        globalConfig.setCardinalityBuckets("1,10,5");
        assertThatThrownBy(() -> new DispatcherStatisticsProvider(transformer, globalConfig));
    }

    @Test
    public void cardinalitySimpleGet()
    {
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setCardinalityBuckets("1,100,300");
        DispatcherStatisticsProvider dispatcherStatisticsProvider = new DispatcherStatisticsProvider(transformer, globalConfig);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(20))).isEqualTo(1);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(110))).isEqualTo(2);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.of(400))).isEqualTo(3);
    }

    @Test
    public void cardinalityUnknownShouldReturnZero()
    {
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setCardinalityBuckets("1,100,300");
        DispatcherStatisticsProvider dispatcherStatisticsProvider = new DispatcherStatisticsProvider(transformer, globalConfig);
        assertThat(dispatcherStatisticsProvider.getColumnCardinalityBucket(Estimate.unknown())).isEqualTo(0);
    }
}
