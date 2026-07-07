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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

final class IcebergTablesUtil
{
    private IcebergTablesUtil() {}

    public static Path findTableDirectory(Path dataLocation, String table)
    {
        try (Stream<Path> dirs = Files.list(dataLocation)) {
            List<Path> matches = dirs
                    .filter(Files::isDirectory)
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.equals(table) || name.startsWith(table + "-");
                    })
                    .toList();
            if (matches.isEmpty()) {
                throw new IllegalStateException("No directory found for table '%s' in %s".formatted(table, dataLocation));
            }
            if (matches.size() > 1) {
                throw new IllegalStateException("Multiple directories found for table '%s' in %s: %s".formatted(table, dataLocation, matches));
            }
            return matches.getFirst();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path resolveTablesLocation(String dataLocation)
    {
        return Path.of(dataLocation, "tables");
    }
}
