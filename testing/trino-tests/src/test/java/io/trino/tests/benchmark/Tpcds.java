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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class Tpcds
{
    private Tpcds() {}

    // Queries whose template defines two query parts, generated as separate "a" and "b" files.
    private static final Set<Integer> TPCDS_QUERIES_WITH_TWO_PARTS = ImmutableSet.of(14, 23, 24, 39);

    public static List<String> allQueries()
    {
        return IntStream.rangeClosed(1, 99)
                .boxed()
                .flatMap(queryNumber -> {
                    if (TPCDS_QUERIES_WITH_TWO_PARTS.contains(queryNumber)) {
                        return Stream.of(format("q%02da", queryNumber), format("q%02db", queryNumber));
                    }
                    return Stream.of(format("q%02d", queryNumber));
                })
                .toList();
    }

    public static String normalizeQuery(String query)
    {
        Matcher matcher = Pattern.compile("(\\d{1,2})|(q\\d\\d[ab]?)").matcher(query);
        checkArgument(matcher.matches(), "Invalid query name: %s", query);
        String fullQueryName = matcher.group(2);
        if (fullQueryName != null) {
            verify(fullQueryName.equals(query));
            return query;
        }
        int queryNumber = Integer.parseInt(matcher.group(1));
        return format("q%02d", queryNumber);
    }

    public static String readQuery(String query, String catalog, String schema)
    {
        requireNonNull(query, "query is null");
        requireNonNull(catalog, "catalog is null");
        requireNonNull(schema, "schema is null");

        try {
            return Resources.toString(
                            getResource("sql/trino/tpcds/%s.sql".formatted(query)), UTF_8)
                    .replace("${database}", catalog)
                    .replace("${schema}", schema)
                    .trim()
                    .replaceFirst(";$", "");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
