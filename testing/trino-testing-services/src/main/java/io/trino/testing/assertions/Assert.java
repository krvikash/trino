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
package io.trino.testing.assertions;

import io.airlift.log.Logger;
import io.airlift.units.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Assert
{
    private static final Logger log = Logger.get(Assert.class);

    private Assert() {}

    public static <E extends Exception> void assertEventually(CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(new Duration(30, SECONDS), assertion);
    }

    public static <E extends Exception> void assertEventually(Duration timeout, CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(timeout, new Duration(50, MILLISECONDS), assertion);
    }

    public static <E extends Exception> void assertEventually(Duration timeout, Duration retryFrequency, CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(timeout, retryFrequency, Integer.MAX_VALUE, 0f, assertion);
    }

    public static <E extends Exception> void assertEventually(Duration timeout, Duration retryFrequency, int maxAttempts, float minSuccessRate, CheckedRunnable<E> assertion)
            throws E
    {
        checkArgument(minSuccessRate >= 0f && minSuccessRate <= 1f, "minSuccessRate must be between 0 and 1");
        int successCount = 0;
        int attemptCount = 0;
        Throwable lastFailure = null;
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                attemptCount++;
                assertion.run();
                successCount++;
                if (((float) successCount / attemptCount) >= minSuccessRate) {
                    return;
                }
                if (Duration.nanosSince(start).compareTo(timeout) > 0 || attemptCount > maxAttempts) {
                    throw new AssertionError(
                            String.format("Success rate %.1f%% is below the minimum required %.1f%%", ((float) successCount / attemptCount) * 100, minSuccessRate * 100),
                            lastFailure);
                }
            }
            catch (Exception | AssertionError e) {
                if (Duration.nanosSince(start).compareTo(timeout) > 0 || attemptCount > maxAttempts) {
                    throw e;
                }
                log.debug(e, "Failure on attempt %s of %s", attemptCount, assertion);
                lastFailure = e;
            }
            try {
                Thread.sleep(retryFrequency.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public static <E extends Exception> void assertConsistently(Duration timeout, Duration retryFrequency, Assert.CheckedRunnable<E> assertion)
            throws E
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            assertion.run();

            if (Duration.nanosSince(start).compareTo(timeout) > 0) {
                return;
            }

            try {
                Thread.sleep(retryFrequency.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public interface CheckedRunnable<E extends Exception>
    {
        void run()
                throws E;
    }
}
