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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import io.trino.spi.TrinoException;
import org.json.JSONArray;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.cloud.bigquery.storage.v1.WriteStream.Type.COMMITTED;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_BAD_WRITE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class BigQueryCommittedWriter
        implements BigQueryWriter
{
    private final BigQueryWriteClient client;
    private final AtomicReference<WriteStream> writeStream = new AtomicReference<>();
    private final CreateWriteStreamRequest createWriteStreamRequest;

    public BigQueryCommittedWriter(BigQueryWriteClient client, String tableName)
    {
        this.client = requireNonNull(client, "client is null");

        WriteStream stream = WriteStream.newBuilder().setType(COMMITTED).build();
        this.createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(tableName)
                .setWriteStream(stream)
                .build();
    }

    @Override
    public void appendBatch(JSONArray batch)
    {
        WriteStream stream = writeStream.updateAndGet(this::getOrCreateWriteStream);
        try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(stream.getName(), stream.getTableSchema(), client).build()) {
            ApiFuture<AppendRowsResponse> future = writer.append(batch);
            AppendRowsResponse response = future.get(); // Throw error
            if (response.hasError()) {
                throw new TrinoException(BIGQUERY_BAD_WRITE, format("Response has error: %s", response.getError().getMessage()));
            }
        }
        catch (Exception e) {
            throw new TrinoException(BIGQUERY_BAD_WRITE, "Failed to insert rows", e);
        }
    }

    private WriteStream getOrCreateWriteStream(WriteStream current)
    {
        if (current == null) {
            return client.createWriteStream(createWriteStreamRequest);
        }
        return current;
    }

    @Override
    public void finish()
    {
        client.close();
    }

    @Override
    public void abort()
    {
        client.close();
    }
}
