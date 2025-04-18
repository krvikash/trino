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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Singleton
public class NativeLogger
{
    public static final int MAX_CATALOGS_FACTOR = 100;
    private static final int MAX_LOG_STRING_LENGTH = 1500;

    private static final SequenceLayout LOGGER_LOG_STRING_LAYOUT;
    private static final StructLayout LOGGER_LOG_LAYOUT;
    public static final long LOGGER_LOG_OFFSET_STATE;
    public static final long LOGGER_LOG_OFFSET_LENGTH;
    private static final long LOGGER_LOG_OFFSET_MAX_LENGTH;
    public static final long LOGGER_LOG_OFFSET_STRING;

    static {
        LOGGER_LOG_STRING_LAYOUT = MemoryLayout.sequenceLayout(MAX_LOG_STRING_LENGTH, ValueLayout.JAVA_BYTE);
        LOGGER_LOG_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.ADDRESS.withName("stateList"),
                ValueLayout.JAVA_INT.withName("length"),
                ValueLayout.JAVA_INT.withName("max_length"),
                LOGGER_LOG_STRING_LAYOUT.withName("string")).withName("logger_log_t");
        LOGGER_LOG_OFFSET_STATE = LOGGER_LOG_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("stateList"));
        LOGGER_LOG_OFFSET_LENGTH = LOGGER_LOG_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("length"));
        LOGGER_LOG_OFFSET_MAX_LENGTH = LOGGER_LOG_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("max_length"));
        LOGGER_LOG_OFFSET_STRING = LOGGER_LOG_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("string"));
    }

    private final BlockingQueue<Integer> idPool;
    private final MemorySegment logMem;
    private final MemorySegment stateListMem;
    private final ShapingLogger shapingLogger;
    private final int numIds;

    @Inject
    public NativeLogger(NativeConfig nativeConfig, ShapingLoggerFactory shapingLoggerFactory)
    {
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
        this.numIds = nativeConfig.getTaskMaxWorkerThreads() * MAX_CATALOGS_FACTOR;
        this.logMem = Arena.global().allocate(LOGGER_LOG_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());
        this.stateListMem = Arena.global().allocate(ValueLayout.JAVA_INT.byteSize() * numIds, ValueLayout.JAVA_INT.byteSize());
        logMem.set(ValueLayout.ADDRESS, LOGGER_LOG_OFFSET_STATE, stateListMem);
        logMem.set(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_MAX_LENGTH, 1500);

        this.idPool = new LinkedBlockingQueue<>();
        for (int i = 0; i < numIds; i++) {
            idPool.add(i);
        }
    }

    public MemorySegment getLogMem()
    {
        return logMem;
    }

    public MemorySegment getStateListMem()
    {
        return stateListMem;
    }

    public LogId getLogId(Optional<ExceptionThrower> exceptionThrower)
    {
        Integer id = idPool.poll();
        if (id == null) {
            synchronized (this) {
                id = idPool.poll();
                if (id == null) {
                    shapingLogger.error("failed to get log id. pool size %d. refill poll", idPool.size());
                    for (int i = 0; i < numIds; i++) {
                        idPool.add(i);
                    }
                    id = idPool.poll();
                }
            }
        }
        return new LogId(id, exceptionThrower);
    }

    public void releaseLogId(Integer id)
    {
        try {
            idPool.add(id);
        }
        catch (Exception e) {
            shapingLogger.error(e, "failed to release log id %d", id);
        }
    }

    public void checkForLogs(Integer id, Optional<ExceptionThrower> exceptionThrower)
    {
        int logLevel = stateListMem.getAtIndex(ValueLayout.JAVA_INT, id);
        if (logLevel == 0) {
            return;
        }
        stateListMem.set(ValueLayout.JAVA_INT, id * ValueLayout.JAVA_INT.byteSize(), 0);

        // We use a shared buffer for all threads, this means that in case we got multiple logs from different
        // threads the first thread to get here will print all the logs, the rest will just throw an exception,
        // therefore, the catalog name might be incorrect in such case.
        // another impact is if while we read the logs another thread is trying to write in native, so we might get a corrupted log.
        // The other thread has a different id so he will throw an exception if needed.
        int logLength = logMem.get(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_LENGTH);
        MemorySegment logStringMem = logMem.asSlice(LOGGER_LOG_OFFSET_STRING);
        String logString;
        if (logLength == 0) {
            logString = "";
        }
        else {
            logString = logStringMem.getString(0);
        }
        logMem.set(ValueLayout.JAVA_INT, LOGGER_LOG_OFFSET_LENGTH, 0);

        if (logLevel < 0) {
            final int expectionId = -1 * logLevel;
            shapingLogger.error("threw native exception %s", logString);
            exceptionThrower.ifPresent(e -> e.throwException(expectionId, "Exception thrown from warp speed native library"));
            while (true) {
                shapingLogger.warn("threw native exception went to endless sleep");
                try {
                    Thread.sleep(100);
                }
                catch (Exception e) {
                    shapingLogger.debug("sleep interrupted");
                }
            }
        }
        else if (!logString.isEmpty() && (logString.length() <= MAX_LOG_STRING_LENGTH)) {
            switch (logLevel) {
                case 1 -> shapingLogger.error("%s", logString);
                case 2 -> shapingLogger.info("%s", logString);
                case 3 -> shapingLogger.debug("%s", logString);
            }
        }
    }

    public class LogId
            implements AutoCloseable
    {
        private final Integer id;
        private final Optional<ExceptionThrower> exceptionThrower;

        public LogId(Integer id, Optional<ExceptionThrower> exceptionThrower)
        {
            this.id = id;
            this.exceptionThrower = exceptionThrower;
        }

        @Override
        public void close()
                throws Exception
        {
            try {
                checkForLogs(id, exceptionThrower);
            }
            finally {
                releaseLogId(id);
            }
        }

        public int id()
        {
            return id;
        }
    }
}
