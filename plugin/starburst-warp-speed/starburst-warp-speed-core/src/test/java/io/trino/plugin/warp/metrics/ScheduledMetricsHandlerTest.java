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
package io.trino.plugin.warp.metrics;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.plugin.warp.config.MetricsConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduledMetricsHandlerTest
{
    @Test
    public void testScheduler()
    {
        MetricsConfig metricsConfig = new MetricsConfig();
        metricsConfig.setIntervalCleanerDuration(Duration.ofMillis(1));
        metricsConfig.setDelayDuration(Duration.ofMillis(1));

        AtomicBoolean isRun = new AtomicBoolean(false);

        MetricsTimerTask metricsTimerTask = new MetricsTimerTask(metricsConfig)
        {
            @Override
            public Duration getInterval()
            {
                return metricsConfig.getIntervalCleanerDuration();
            }

            @Override
            public void run()
            {
                isRun.set(true);
            }
        };

        ScheduledMetricsHandler scheduledMetricsHandler = new ScheduledMetricsHandler();

        scheduledMetricsHandler.scheduleMetricsTimerTask(metricsTimerTask);

        Failsafe.with(RetryPolicy.builder()
                        .handle(AssertionError.class)
                        .withMaxRetries(50)
                        .withDelay(Duration.ofMillis(2))
                        .withMaxDuration(Duration.ofMillis(100))
                        .build())
                .run(() -> assertThat(isRun).isTrue());
    }
}
