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
package io.trino.plugin.warp.dispatcher;

import org.slf4j.MDC;

import java.util.Optional;

public class WarpMDCContext
        implements AutoCloseable
{
    public static final String QUERY_ID_LOCAL_PROPERTY = "QUERY_ID";
    public static final String CATALOG_NAME_LOCAL_PROPERTY = "CATALOG_NAME";

    public WarpMDCContext(String catalogName, Optional<String> optQueryId)
    {
        optQueryId.ifPresent(queryId -> MDC.put(QUERY_ID_LOCAL_PROPERTY, queryId));
        MDC.put(CATALOG_NAME_LOCAL_PROPERTY, catalogName);
    }

    @Override
    public void close()
    {
        MDC.remove(QUERY_ID_LOCAL_PROPERTY);
        MDC.remove(CATALOG_NAME_LOCAL_PROPERTY);
    }
}
