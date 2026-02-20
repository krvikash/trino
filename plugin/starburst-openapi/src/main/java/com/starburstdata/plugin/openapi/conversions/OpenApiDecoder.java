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
package com.starburstdata.plugin.openapi.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.starburstdata.plugin.openapi.OpenApiColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;

public interface OpenApiDecoder
{
    OpenApiDecoder ONE_COLUMN_DECODER = new OneColumnDecoder();

    List<OpenApiColumnHandle> getColumnHandles();

    Page decodeToPage(JsonNode root, List<ColumnHandle> columnHandles)
            throws DecodingException;

    class DecodingException
            extends Exception
    {
        public DecodingException(String message)
        {
            super(message);
        }
    }
}
