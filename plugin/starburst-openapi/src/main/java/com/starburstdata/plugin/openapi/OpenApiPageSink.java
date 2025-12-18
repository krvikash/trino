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
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class OpenApiPageSink
        implements ConnectorPageSink
{
    protected final OpenApiClient client;
    protected final SchemaTableName tableName;
    protected final PathItem.HttpMethod updateMethod;
    protected final List<String> insertPaths;
    protected final List<String> updatePaths;
    protected final TupleDomain<ColumnHandle> constraint;

    public OpenApiPageSink(OpenApiClient client, OpenApiTableHandle table)
    {
        this.client = requireNonNull(client, "client is null");
        tableName = table.getSchemaTableName();
        updateMethod = table.getUpdateMethod();
        insertPaths = table.getInsertPaths();
        updatePaths = table.getUpdatePaths();
        constraint = table.getConstraint();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            insertedPage(page, position);
        }
        return NOT_BLOCKED;
    }

    protected void insertedPage(Page page, int position)
    {
        switch (updateMethod) {
            case POST -> client.postRows(tableName, insertPaths, constraint, page, position);
            case PUT -> client.putRows(tableName, updatePaths, constraint, page, position);
            default -> throw new IllegalArgumentException("Unsupported INSERT method: " + updateMethod);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
    }
}
