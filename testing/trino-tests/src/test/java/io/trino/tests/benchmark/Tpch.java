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

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class Tpch
{
    private Tpch() {}

    public static List<String> allQueries()
    {
        return IntStream.rangeClosed(1, 22)
                .boxed()
                .map(queryNumber -> format("q%02d", queryNumber))
                .toList();
    }

    public static String readQuery(String query, String catalog, String schema, int scaleFactor)
    {
        requireNonNull(query, "query is null");
        requireNonNull(catalog, "catalog is null");
        requireNonNull(schema, "schema is null");

        try {
            return Resources.toString(
                            getResource("sql/trino/tpch/%s.sql".formatted(query)), UTF_8)
                    .replace("${database}", catalog)
                    .replace("${schema}", schema)
                    .replace("${prefix}", "")
                    .replace("${scale}", String.valueOf(scaleFactor))
                    .trim()
                    .replaceFirst(";$", "");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
