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
package io.trino.execution.scheduler.faulttolerant;

import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class QueryRestartDelay
{
    private static final double MAX_JITTER = 0.25;

    private QueryRestartDelay() {}

    static Duration restartDelay(Duration initialDelay, Duration maxDelay, double delayScaleFactor, int completedAttempt, double jitter)
    {
        checkArgument(jitter >= 0 && jitter <= 1, "jitter must be in [0, 1] but is %s", jitter);
        double delayMillis = min(initialDelay.toMillis() * pow(delayScaleFactor, completedAttempt), maxDelay.toMillis());
        // One lost worker takes down every query that had a chunk on it, and all of them would otherwise compute the same
        // delay from the same instant and re-plan on the coordinator at once. Spread the restarts over +-25% of the delay.
        double jitterFactor = 1.0 - MAX_JITTER + 2 * MAX_JITTER * jitter;
        return new Duration(delayMillis * jitterFactor, MILLISECONDS);
    }
}
