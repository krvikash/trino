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
package io.trino.jdbc;

import io.trino.client.SerializationShim;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.client.CloseableIterator.closeable;

// This is a hack to avoid shading issues with relocated client classes
public class StargateInMemoryResultSet
        extends AbstractTrinoResultSet
{
    private final AtomicBoolean closed = new AtomicBoolean();

    public StargateInMemoryResultSet(String serializedColumns, Iterator<List<Object>> iterator)
    {
        super(Optional.empty(), SerializationShim.toColumns(serializedColumns), closeable(iterator));
    }

    @Override
    public void close()
            throws SQLException
    {
        closed.set(true);
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }
}
