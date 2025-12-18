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

import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;

import java.util.List;

import static io.trino.spi.type.TinyintType.TINYINT;

public class OpenApiMergeSink
        extends OpenApiPageSink
        implements ConnectorMergeSink
{
    private final PathItem.HttpMethod deleteMethod;
    private final List<String> deletePaths;

    public OpenApiMergeSink(OpenApiClient client, OpenApiTableHandle table)
    {
        super(client, table);
        deleteMethod = table.deleteMethod();
        deletePaths = table.deletePaths();
    }

    @Override
    public void storeMergedRows(Page page)
    {
        Block ops = page.getBlock(page.getChannelCount() - 3);
        for (int position = 0; position < page.getPositionCount(); position++) {
            byte op = TINYINT.getByte(ops, position);
            switch (op) {
                case INSERT_OPERATION_NUMBER -> insertedPage(page, position);
                case UPDATE_OPERATION_NUMBER -> updatedPage(page, position);
                case DELETE_OPERATION_NUMBER -> deletedPage();
                default -> throw new IllegalStateException("Unsupported operation: " + op);
            }
        }
    }

    private void updatedPage(Page page, int position)
    {
        switch (updateMethod) {
            case PUT -> client.putRows(tableName, updatePaths, constraint, page, position);
            case POST -> client.postRows(tableName, insertPaths, constraint, page, position);
            default -> throw new IllegalArgumentException("Unsupported UPDATE method: " + updateMethod);
        }
    }

    private void deletedPage()
    {
        if (deleteMethod == PathItem.HttpMethod.DELETE) {
            client.deleteRows(tableName, deletePaths, constraint);
        }
        else {
            throw new IllegalArgumentException("Unsupported DELETE method: " + deleteMethod);
        }
    }
}
