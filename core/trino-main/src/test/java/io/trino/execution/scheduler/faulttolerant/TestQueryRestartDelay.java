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
import org.junit.jupiter.api.Test;

import static io.trino.execution.scheduler.faulttolerant.QueryRestartDelay.restartDelay;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryRestartDelay
{
    private static final Duration INITIAL_DELAY = new Duration(10, SECONDS);
    private static final Duration MAX_DELAY = new Duration(60, SECONDS);
    // the midpoint of the jitter range leaves the computed delay unchanged
    private static final double NO_JITTER = 0.5;

    @Test
    public void testRestartDelayGrowsWithAttempts()
    {
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 0, NO_JITTER).toMillis()).isEqualTo(10_000);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 1, NO_JITTER).toMillis()).isEqualTo(20_000);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 2, NO_JITTER).toMillis()).isEqualTo(40_000);
    }

    @Test
    public void testRestartDelayIsCapped()
    {
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 3, NO_JITTER).toMillis()).isEqualTo(MAX_DELAY.toMillis());
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 100, NO_JITTER).toMillis()).isEqualTo(MAX_DELAY.toMillis());
    }

    @Test
    public void testRestartDelayWithoutScaling()
    {
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 1.0, 5, NO_JITTER).toMillis()).isEqualTo(INITIAL_DELAY.toMillis());
    }

    @Test
    public void testRestartDelayCanBeDisabled()
    {
        assertThat(restartDelay(new Duration(0, SECONDS), MAX_DELAY, 2.0, 4, NO_JITTER).toMillis()).isEqualTo(0);
        assertThat(restartDelay(new Duration(0, SECONDS), MAX_DELAY, 2.0, 4, 1.0).toMillis()).isEqualTo(0);
    }

    @Test
    public void testRestartDelayIsJittered()
    {
        // queries hit by the same worker loss must not all restart in the same instant
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 0, 0.0).toMillis()).isEqualTo(7_500);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 0, 0.5).toMillis()).isEqualTo(10_000);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 0, 1.0).toMillis()).isEqualTo(12_500);
        // jitter applies to the capped delay, so the spread does not collapse once the cap is reached
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 100, 0.0).toMillis()).isEqualTo(45_000);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 100, 0.5).toMillis()).isEqualTo(60_000);
        assertThat(restartDelay(INITIAL_DELAY, MAX_DELAY, 2.0, 100, 1.0).toMillis()).isEqualTo(75_000);
    }
}
