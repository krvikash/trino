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
package io.trino.testing.services.junit;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.trino.testing.services.junit.Listeners.reportListenerFailure;
import static java.util.stream.Collectors.joining;

public class ReportGpuMemoryLeaks
        implements TestExecutionListener
{
    private static final String ENABLED_PROPERTY = "ReportGpuMemoryLeaks.enabled";
    private static final String EAGER_PROPERTY = "ReportGpuMemoryLeaks.eager";

    private final boolean enabled;
    private final boolean eager;

    @GuardedBy("this")
    private Logger cudfLogger;
    @GuardedBy("this")
    private LeakDetectionHandler leakHandler;

    public ReportGpuMemoryLeaks()
    {
        enabled = System.getProperty(ENABLED_PROPERTY) == null || Boolean.getBoolean(ENABLED_PROPERTY);
        eager = Boolean.getBoolean(EAGER_PROPERTY);
    }

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan)
    {
        try {
            if (!enabled) {
                return;
            }

            synchronized (this) {
                checkState(leakHandler == null, "leakHandler already set");
                checkState(cudfLogger == null, "cudfLogger already set");
                leakHandler = new LeakDetectionHandler();
                cudfLogger = Logger.getLogger("ai.rapids.cudf");
                cudfLogger.addHandler(leakHandler);
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "testPlanExecutionStarted: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult)
    {
        try {
            if (!enabled) {
                return;
            }

            if (eager) {
                System.gc();
            }
            reportLeaks(Optional.of(testIdentifier));
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "executionFinished: \n%s", getStackTraceAsString(e));
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        try {
            if (!enabled) {
                return;
            }

            // Give the Cleaner Thread a grace period to detect and log any leaks
            System.gc();
            System.gc();
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            synchronized (this) {
                try {
                    checkState(leakHandler != null, "leakHandler not set");
                    cudfLogger.removeHandler(leakHandler);
                    reportLeaks(Optional.empty());
                }
                finally {
                    leakHandler = null;
                    cudfLogger = null;
                }
            }
        }
        catch (RuntimeException | Error e) {
            reportListenerFailure(getClass(), "testPlanExecutionFinished: \n%s", getStackTraceAsString(e));
        }
    }

    private void reportLeaks(Optional<TestIdentifier> lastTest)
    {
        List<String> leaks;
        synchronized (this) {
            leaks = leakHandler.drainLeaks();
        }
        if (!leaks.isEmpty()) {
            String message = "GPU memory leaks detected";
            if (lastTest.isPresent()) {
                message += " after finishing " + lastTest.get();
                message += " (leak detection is deferred, try running with -D%s=true for higher accuracy)".formatted(EAGER_PROPERTY);
            }
            message += ": %s leak(s)".formatted(leaks.size());
            message += leaks.stream()
                    .collect(joining("\n  ", "\n  ", ""));
            reportListenerFailure(getClass(), "%s", message);
        }
    }

    private static class LeakDetectionHandler
            extends Handler
    {
        @GuardedBy("this")
        private final List<String> leakMessages = new ArrayList<>();

        @Override
        public void publish(LogRecord record)
        {
            String message = record.getMessage();
            if (message != null && (message.contains("LEAKED") || message.matches("(?s:.*)Task .* reached state .* but still holds .* memory(?s:.*)"))) {
                String formatted = formatLeakMessage(record);
                synchronized (this) {
                    leakMessages.add(formatted);
                }
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        public synchronized List<String> drainLeaks()
        {
            List<String> current = ImmutableList.copyOf(leakMessages);
            leakMessages.clear();
            return current;
        }
    }

    private static String formatLeakMessage(LogRecord record)
    {
        return String.format("[%s] %s", record.getLevel(), record.getMessage());
    }
}
