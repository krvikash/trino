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
package io.trino.tests.benchmark;

import ai.rapids.cudf.RmmEventHandler;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class AllocationTraceHandler
        implements RmmEventHandler
{
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    // Thread.threadId() returns a JVM-internal sequence number, not the OS thread ID.
    // RMM's spdlog uses gettid() for its Thread column, so we must call gettid() here
    // to produce matching thread IDs for GpuMemoryAnalyzer correlation.
    private static final MethodHandle GETTID;

    static {
        try {
            GETTID = Linker.nativeLinker()
                    .downcallHandle(
                            Linker.nativeLinker().defaultLookup().find("gettid").orElseThrow(),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT));
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long thresholdBytes;
    private final PrintWriter writer;

    public AllocationTraceHandler(long thresholdBytes, File traceFile)
    {
        this.thresholdBytes = thresholdBytes;
        try {
            this.writer = new PrintWriter(Files.newBufferedWriter(traceFile.toPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writer.println("Timestamp\tThread\tAction\tSize\tStackFrames");
    }

    @Override
    public void onAllocated(long size)
    {
        if (size >= thresholdBytes) {
            writeEntry("allocate", size);
        }
    }

    @Override
    public void onDeallocated(long size)
    {
        if (size >= thresholdBytes) {
            writeEntry("deallocate", size);
        }
    }

    @Override
    public boolean onAllocFailure(long sizeRequested, int retryCount)
    {
        writeEntry("allocate_failed", sizeRequested);
        return false;
    }

    private void writeEntry(String action, long size)
    {
        String timestamp = LocalTime.now().format(TIMESTAMP_FORMAT);
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StringBuilder frames = new StringBuilder();
        for (int i = 2; i < stack.length; i++) {
            if (i > 2) {
                frames.append('|');
            }
            frames.append(stack[i]);
        }
        writer.println(timestamp + "\t" + nativeThreadId() + "\t" + action + "\t" + size + "\t" + frames);
    }

    private static int nativeThreadId()
    {
        try {
            return (int) GETTID.invokeExact();
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void flush()
    {
        writer.flush();
    }

    // Alloc/dealloc thresholds are watermarks on total allocated memory (e.g. fire when
    // total usage crosses 1 GB). They are unrelated to thresholdBytes, which filters
    // individual allocation size. Returning null disables threshold callbacks.

    @Override
    public long[] getAllocThresholds()
    {
        return null;
    }

    @Override
    public long[] getDeallocThresholds()
    {
        return null;
    }

    @Override
    public void onAllocThreshold(long totalAllocSize) {}

    @Override
    public void onDeallocThreshold(long totalAllocSize) {}
}
