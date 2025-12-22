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
package io.trino.plugin.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import io.trino.spi.TrinoException;
import org.json.JSONArray;

import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.cloud.bigquery.storage.v1.WriteStream.Type.PENDING;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_BAD_WRITE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

final class BigQueryPendingWriter
        implements BigQueryWriter
{
    // Single thread is good enough here, as callback is very light-weight
    private static final Executor CALLBACK_EXECUTOR = newSingleThreadExecutor(daemonThreadsNamed("bigquery-callback-listener-%s"));

    private final BigQueryWriteClient client;
    private final AtomicReference<WriteStream> writeStream = new AtomicReference<>();
    private final CreateWriteStreamRequest createWriteStreamRequest;

    private final BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequestBuilder;
    private final Phaser inFlight = new Phaser(1); // Register self
    private final FailureRecorder failureRecorder = new FailureRecorder();

    // Reuse writer (for better performance)
    private JsonStreamWriter jsonStreamWriter;

    public BigQueryPendingWriter(BigQueryWriteClient client, String tableName)
    {
        this.client = requireNonNull(client, "client is null");

        WriteStream stream = WriteStream.newBuilder().setType(PENDING).build();
        this.createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(tableName)
                .setWriteStream(stream)
                .build();

        this.batchCommitWriteStreamsRequestBuilder = BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(tableName);
    }

    @Override
    public void appendBatch(JSONArray batch)
    {
        failureRecorder.throwIfPresent();
        inFlight.register();
        try {
            // Non-blocking append
            ApiFuture<AppendRowsResponse> future = getOrCreateJsonStreamWriter().append(batch);
            ApiFutures.addCallback(future, new AppendCompleteCallback(failureRecorder, inFlight), CALLBACK_EXECUTOR);
        }
        catch (Exception e) {
            inFlight.arriveAndDeregister();
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to insert rows", e);
        }
    }

    private synchronized JsonStreamWriter getOrCreateJsonStreamWriter()
    {
        if (jsonStreamWriter == null) {
            WriteStream stream = writeStream.updateAndGet(this::getOrCreateWriteStream);
            try {
                jsonStreamWriter = JsonStreamWriter.newBuilder(stream.getName(), stream.getTableSchema(), client).build();
            }
            catch (Exception e) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to create json stream writer", e);
            }
        }
        return jsonStreamWriter;
    }

    private WriteStream getOrCreateWriteStream(WriteStream current)
    {
        if (current == null) {
            WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);
            batchCommitWriteStreamsRequestBuilder.addWriteStreams(writeStream.getName());
            return writeStream;
        }
        return current;
    }

    @Override
    public void finish()
    {
        // Wait for all async appends
        inFlight.arriveAndAwaitAdvance();
        failureRecorder.throwIfPresent();

        try {
            if (jsonStreamWriter != null) {
                jsonStreamWriter.close();
            }

            WriteStream stream = writeStream.get();
            if (stream != null) {
                client.finalizeWriteStream(stream.getName());
                BatchCommitWriteStreamsResponse response = client.batchCommitWriteStreams(batchCommitWriteStreamsRequestBuilder.build());
                if (!response.hasCommitTime()) {
                    throw new TrinoException(BIGQUERY_BAD_WRITE, "Pending write stream commit failed");
                }
            }
        }
        catch (Exception e) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to finish BigQuery write", e);
        }
        finally {
            client.close();
        }
    }

    @Override
    public void abort()
    {
        // We don't wait for in-flight operations to complete here because abort is typically called in
        // failure scenarios or cancellation where we want to release resources immediately.
        // Any pending futures will likely fail when the client/writer is closed, triggering their callbacks
        // which will deregister from the phaser.
        try {
            if (jsonStreamWriter != null) {
                jsonStreamWriter.close();
            }
        }
        finally {
            client.close();
        }
    }

    private static final class AppendCompleteCallback
            implements ApiFutureCallback<AppendRowsResponse>
    {
        private final FailureRecorder failureRecorder;
        private final Phaser inFlight;

        AppendCompleteCallback(FailureRecorder failureRecorder, Phaser inFlight)
        {
            this.failureRecorder = requireNonNull(failureRecorder, "failureRecorder is null");
            this.inFlight = requireNonNull(inFlight, "inFlight is null");
        }

        @Override
        public void onSuccess(AppendRowsResponse response)
        {
            if (response.hasError()) {
                failureRecorder.record(new TrinoException(BIGQUERY_BAD_WRITE, format("Response has error: %s", response.getError().getMessage())));
            }
            inFlight.arriveAndDeregister();
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            failureRecorder.record(new TrinoException(BIGQUERY_BAD_WRITE, "Failed to insert rows", throwable));
            inFlight.arriveAndDeregister();
        }
    }

    private static final class FailureRecorder
    {
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        void record(Throwable throwable)
        {
            failure.compareAndSet(null, throwable);
        }

        void throwIfPresent()
        {
            Throwable throwable = failure.get();
            if (throwable != null) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, throwable);
            }
        }
    }
}
