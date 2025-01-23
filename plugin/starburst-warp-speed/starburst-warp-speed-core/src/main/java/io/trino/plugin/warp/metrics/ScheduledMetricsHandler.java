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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;

import java.util.Timer;
import java.util.concurrent.TimeUnit;

@Singleton
public class ScheduledMetricsHandler
{
    private static final Logger logger = Logger.get(ScheduledMetricsHandler.class);

    private final Timer timer;

    @Inject
    public ScheduledMetricsHandler()
    {
        this.timer = new Timer();
    }

    public void scheduleMetricsTimerTask(MetricsTimerTask metricsTimerTask)
    {
        logger.debug("init metricsTimerTask");

        long delayMillis = metricsTimerTask.getDelay().toSeconds() > 0
                ? TimeUnit.SECONDS.toMillis(metricsTimerTask.getDelay().toSeconds())
                : TimeUnit.NANOSECONDS.toMillis(metricsTimerTask.getDelay().toNanosPart());

        long intervalInSeconds = metricsTimerTask.getInterval().toSeconds();
        int intervalInNanos = metricsTimerTask.getInterval().toNanosPart();
        long intervalMillis = intervalInSeconds > 0 ? TimeUnit.SECONDS.toMillis(intervalInSeconds) : TimeUnit.NANOSECONDS.toMillis(intervalInNanos);

        timer.scheduleAtFixedRate(metricsTimerTask, delayMillis, intervalMillis);
    }
}
