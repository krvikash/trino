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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_ERROR;
import static io.trino.plugin.warp.storage.engine.nativeimpl.NativeLogger.LOGGER_LOG_OFFSET_LENGTH;
import static io.trino.plugin.warp.storage.engine.nativeimpl.NativeLogger.LOGGER_LOG_OFFSET_STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Execution(ExecutionMode.SAME_THREAD) // Ensures tests run sequentially
public class NativeLoggerTest
{
    private NativeConfig nativeConfig;
    private NativeLogger nativeLogger;
    private CatalogName catalogName;
    private ExceptionThrower exceptionThrower;
    private static final Logger logger = Logger.getLogger(NativeLogger.class.getName());
    private TestLogHandler logHandler;

    @BeforeEach
    public void beforeEach()
    {
        nativeConfig = new NativeConfig();
        nativeConfig.setTaskMaxWorkerThreads(3);
        catalogName = new CatalogName("default_catalog_name");

        ShapingLoggerFactory shapingLoggerFactory = new ShapingLoggerFactory(catalogName, new SharedConfig());
        nativeLogger = new NativeLogger(nativeConfig, shapingLoggerFactory);
        MetricsManager metricsManager = mock(MetricsManager.class);
        exceptionThrower = new NativeExceptionThrower(metricsManager);

        logHandler = new TestLogHandler();
        logger.addHandler(logHandler);
        logger.setUseParentHandlers(false);
    }

    @AfterEach
    void tearDown()
    {
        logger.removeHandler(logHandler);
        logHandler.close();
    }

    private void setLog(Integer id, int logLevel, String logString)
    {
        logString += "\n";
        MemorySegment logMem = nativeLogger.getLogMem();
        MemorySegment logIdList = nativeLogger.getStateListMem();
        logIdList.setAtIndex(ValueLayout.JAVA_INT, id, logLevel);

        int curOffset = logMem.get(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_LENGTH);
        logMem.setString(LOGGER_LOG_OFFSET_STRING + curOffset, logString);
        logMem.set(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_LENGTH, curOffset + logString.length());
    }

    @Test
    void testCheckLogLevel0()
            throws Exception
    {
        String errorMsg = "test1";
        try (NativeLogger.LogId logId = nativeLogger.getLogId(Optional.of(exceptionThrower))) {
            setLog(logId.id(), 0, errorMsg);
        }
        assertThat(logHandler.isEmpty()).isTrue();
    }

    @Test
    void testCheckLogLevelError()
            throws Exception
    {
        String errMsg = "test error";
        try (NativeLogger.LogId logId = nativeLogger.getLogId(Optional.of(exceptionThrower))) {
            setLog(logId.id(), 1, errMsg);
        }
        LogRecord log = logHandler.popLast();
        assertThat(log.getMessage().contains(errMsg)).isTrue();
        assertThat(log.getLevel().equals(Level.SEVERE)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();
    }

    @Test
    void testCheckLogLevelInfo()
            throws Exception
    {
        String infoMsg = "test info";
        try (NativeLogger.LogId logId = nativeLogger.getLogId(Optional.of(exceptionThrower))) {
            setLog(logId.id(), 2, infoMsg);
        }
        LogRecord log = logHandler.popLast();
        assertThat(log.getMessage().contains(infoMsg)).isTrue();
        assertThat(log.getLevel().equals(Level.INFO)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();
    }

    @Test
    void testCheckLogLevelPanic()
    {
        String panicMsg = "test panic";
        NativeLogger.LogId logId = nativeLogger.getLogId(Optional.of(exceptionThrower));
        setLog(logId.id(), -1, panicMsg);
        TrinoException exception = Assertions.assertThrows(TrinoException.class, logId::close);
        assertThat(exception.getErrorCode().equals(WARP_NATIVE_ERROR.toErrorCode())).isTrue();
        LogRecord log = logHandler.popLast();
        assertThat(log.getMessage().contains(panicMsg)).isTrue();
        assertThat(log.getLevel().equals(Level.SEVERE)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();
    }

    @Test
    void testCheckLogLevelPanic2Ids1panic()
            throws Exception
    {
        String panicMsg = "test panic";
        NativeLogger.LogId logId1 = nativeLogger.getLogId(Optional.of(exceptionThrower));
        NativeLogger.LogId logId2 = nativeLogger.getLogId(Optional.of(exceptionThrower));
        setLog(logId1.id(), -1, panicMsg);
        logId2.close();
        TrinoException exception = Assertions.assertThrows(TrinoException.class, logId1::close);
        assertThat(exception.getErrorCode().equals(WARP_NATIVE_ERROR.toErrorCode())).isTrue();
        LogRecord log = logHandler.popLast();
        assertThat(log.getMessage().contains(panicMsg)).isTrue();
        assertThat(log.getLevel().equals(Level.SEVERE)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();
    }

    @Test
    void testCheckLogLevelPanic2Ids2panics()
    {
        String panicMsg1 = "test panic 1";
        String panicMsg2 = "test panic 2";
        NativeLogger.LogId logId1 = nativeLogger.getLogId(Optional.of(exceptionThrower));
        NativeLogger.LogId logId2 = nativeLogger.getLogId(Optional.of(exceptionThrower));
        setLog(logId1.id(), -1, panicMsg1);
        setLog(logId2.id(), -2, panicMsg2);

        // first call to checkForLogs will print all set logs, while the exception should be thrown from every failed call
        TrinoException exception = Assertions.assertThrows(TrinoException.class, logId1::close);
        assertThat(exception.getErrorCode().equals(WARP_NATIVE_ERROR.toErrorCode())).isTrue();
        LogRecord log = logHandler.popLast();
        assertThat(log.getMessage().contains(panicMsg1)).isTrue();
        assertThat(log.getMessage().contains(panicMsg2)).isTrue();
        assertThat(log.getLevel().equals(Level.SEVERE)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();

        exception = Assertions.assertThrows(TrinoException.class, logId2::close);
        assertThat(exception.getErrorCode().equals(WARP_NATIVE_ERROR.toErrorCode())).isTrue();
        log = logHandler.popLast();
        assertThat(log.getMessage().contains(panicMsg1)).isFalse();
        assertThat(log.getMessage().contains(panicMsg2)).isFalse();
        assertThat(log.getLevel().equals(Level.SEVERE)).isTrue();
        assertThat(logHandler.isEmpty()).isTrue();
    }

    static class TestLogHandler
            extends Handler
    {
        private final List<LogRecord> logs = new ArrayList<>();

        @Override
        public void publish(LogRecord record)
        {
            logs.add(record);
        }

        @Override
        public void flush()
        {
        }

        @Override
        public void close()
        {
        }

        public LogRecord popLast()
        {
            return logs.removeLast();
        }

        public boolean isEmpty()
        {
            return logs.isEmpty();
        }
    }
}
