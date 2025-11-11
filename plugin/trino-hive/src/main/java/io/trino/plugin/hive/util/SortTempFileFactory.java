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
package io.trino.plugin.hive.util;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.PageStreamReader;
import io.trino.spi.PageStreamWriter;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

public class SortTempFileFactory
{
    public PageStreamWriter createWriter(List<Type> types, TrinoFileSystem fileSystem, Location tempFile)
            throws IOException
    {
        return new TempFileWriter(types, fileSystem, tempFile);
    }

    public PageStreamReader createReader(List<Type> types, TrinoFileSystem fileSystem, Location tempFile)
    {
        return new TempFileReader(types, fileSystem, tempFile);
    }
}
