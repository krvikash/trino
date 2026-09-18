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

import io.trino.plugin.warp.config.MetricsConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduledMetricsHandlerTest
{
    @Test
    public void testScheduler()
            throws InterruptedException
    {
        MetricsConfig metricsConfig = new MetricsConfig();
        metricsConfig.setIntervalCleanerDuration(Duration.ofMillis(1));
        metricsConfig.setDelayDuration(Duration.ofMillis(1));

        CountDownLatch taskRan = new CountDownLatch(1);

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
                taskRan.countDown();
            }
        };

        ScheduledMetricsHandler scheduledMetricsHandler = new ScheduledMetricsHandler();

        scheduledMetricsHandler.scheduleMetricsTimerTask(metricsTimerTask);

        // The first Timer fire can lag well past the 1ms delay on a loaded runner, so wait on the task itself
        assertThat(taskRan.await(10, TimeUnit.SECONDS))
                .as("scheduled metrics task did not run within timeout")
                .isTrue();
    }
}
