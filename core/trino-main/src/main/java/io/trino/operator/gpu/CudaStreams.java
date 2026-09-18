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
package io.trino.operator.gpu;

import ai.rapids.cudf.Cuda;
import io.airlift.log.Logger;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import static java.util.Objects.requireNonNull;

public final class CudaStreams
{
    private CudaStreams() {}

    private static final Logger log = Logger.get(CudaStreams.class);

    /// Distinct from the exit code 100 that [io.trino.server.Server] uses for startup failures.
    private static final int CUDA_SYNC_FAILURE_EXIT_CODE = 101;

    /// Synchronizes the stream, aborting the process if the synchronization fails.
    ///
    /// A failed `cudaStreamSynchronize` leaves DMAs potentially in flight, so the buffers they
    /// read from and write into can no longer be freed: doing so is undefined behavior. The JVM
    /// offers no way to keep them alive either, because cudf's `MemoryCleaner` frees whatever the
    /// garbage collector reclaims, from an unrelated thread. Terminating is the only response that
    /// keeps the process out of undefined behavior.
    ///
    /// Call this instead of [Cuda.Stream#sync()], which modernizer rejects.
    public static void sync(Cuda.Stream stream)
    {
        requireNonNull(stream, "stream is null");
        try {
            syncOrThrow(stream);
        }
        catch (Throwable e) {
            abort(e);
        }
    }

    @SuppressModernizer // Usage of `Cuda.Stream.sync` is not allowed. Only this method can call it.
    private static void syncOrThrow(Cuda.Stream stream)
    {
        stream.sync();
    }

    private static void abort(Throwable failure)
    {
        try {
            String message = "CUDA stream synchronization failed; terminating the process to avoid undefined behavior";
            // Also write to stderr, in case the logger is not configured yet
            System.err.println(message);
            log.error(failure, "%s", message);
        }
        catch (Throwable ignored) {
            // Reporting is best effort; failing to report must not keep the process alive
        }
        finally {
            System.exit(CUDA_SYNC_FAILURE_EXIT_CODE);
        }
    }
}
