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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.params.provider.Arguments;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static java.util.stream.Collectors.joining;

final class GpuWindowFrameTestParameters
{
    private GpuWindowFrameTestParameters() {}

    /// Aggregations, whose results depend on the frame
    static final List<String> AGGREGATIONS = List.of(
            "avg(argument_decimal_9_2)",
            "avg(argument_decimal_19_4)",
            "avg(argument_decimal_28_2)",
            "avg(argument_decimal_38_6)");

    /// Aggregations that the GPU does not support
    static final List<String> GPU_UNSUPPORTED_AGGREGATIONS = List.of(
            "sum(argument_decimal_38_6)",
            "avg(CAST(argument_decimal_38_6 AS bigint))",
            "avg(CAST(argument_decimal_38_6 AS real))",
            "avg(CAST(argument_decimal_38_6 AS double))");

    static final List<String> ORDERINGS = Arrays.stream(SortOrder.values()).map(SortOrder::toString).toList();

    /// Aggregations of decimals that the GPU computes as differences of prefix sums
    static final List<String> FRAME_SUM_FUNCTIONS = List.of("avg");

    /// Short and long decimals, across the precisions at which the GPU computes the frame sum functions differently
    static final List<DecimalType> FRAME_SUM_DECIMAL_TYPES = List.of(
            createDecimalType(1, 0),
            createDecimalType(18, 0),
            createDecimalType(18, 9),
            createDecimalType(18, 18),
            createDecimalType(19, 0),
            createDecimalType(19, 10),
            createDecimalType(28, 0),
            createDecimalType(28, 14),
            createDecimalType(28, 28),
            createDecimalType(29, 0),
            createDecimalType(29, 2),
            createDecimalType(29, 6),
            createDecimalType(29, 29),
            createDecimalType(38, 0),
            createDecimalType(38, 2),
            createDecimalType(38, 6),
            createDecimalType(38, 38));

    static Stream<Arguments> frameSumFunctionsAndDecimalSumTypes()
    {
        return cartesianProduct(
                FRAME_SUM_FUNCTIONS,
                List.of(
                        createDecimalType(18, 0),
                        createDecimalType(28, 0),
                        createDecimalType(29, 0),
                        createDecimalType(29, 2),
                        createDecimalType(29, 6),
                        createDecimalType(29, 29)));
    }

    static Stream<Arguments> frameSumFunctionsAndOrderings()
    {
        return cartesianProduct(FRAME_SUM_FUNCTIONS, ORDERINGS);
    }

    static IntStream decimalPrecisions()
    {
        return IntStream.rangeClosed(1, MAX_PRECISION);
    }

    /// A query of the aggregation function of a decimal column per type, each holding the unscaled values of the
    /// rows, over each window, such as {@code PARTITION BY partition_key ORDER BY order_key}. The input has columns
    /// {@code row_id}, {@code partition_key}, {@code order_key} and {@code value_<column>}.
    static String decimalAggregationQuery(String function, List<DecimalType> types, List<DecimalRow> rows, List<String> windows)
    {
        String functions = IntStream.range(0, types.size()).boxed()
                .flatMap(column -> windows.stream().map(window -> "%s(value_%s) OVER (%s)".formatted(function, column, window)))
                .collect(joining(",\n"));
        String values = IntStream.range(0, rows.size())
                .mapToObj(rowId -> Stream.concat(
                                Stream.of(rowId, rows.get(rowId).partitionKey(), rows.get(rowId).orderKey())
                                        .map(value -> value == null ? "CAST(NULL AS bigint)" : "BIGINT '%s'".formatted(value)),
                                types.stream().map(type -> decimalLiteral(type, rows.get(rowId).unscaledValue())))
                        .collect(joining(", ", "(", ")")))
                .collect(joining(",\n"));
        String valueColumns = IntStream.range(0, types.size())
                .mapToObj(column -> "value_" + column)
                .collect(joining(", "));
        return """
               SELECT
                   row_id,
               %s
               FROM (
                   VALUES
               %s)
                   input(row_id, partition_key, order_key, %s)
               """.formatted(functions.indent(4).stripTrailing(), values.indent(8).stripTrailing(), valueColumns);
    }

    private static String decimalLiteral(DecimalType type, BigInteger unscaledValue)
    {
        if (unscaledValue == null) {
            return "CAST(NULL AS %s)".formatted(type.getDisplayName());
        }
        return "CAST(DECIMAL '%s' AS %s)".formatted(new BigDecimal(unscaledValue, type.getScale()).toPlainString(), type.getDisplayName());
    }

    // Each list is a partition, with order keys increasing from the start of the input
    static List<DecimalRow> partitions(List<List<BigInteger>> partitions)
    {
        ImmutableList.Builder<DecimalRow> rows = ImmutableList.builder();
        long orderKey = 0;
        for (int partitionKey = 0; partitionKey < partitions.size(); partitionKey++) {
            for (BigInteger value : partitions.get(partitionKey)) {
                rows.add(new DecimalRow((long) partitionKey, orderKey++, value));
            }
        }
        return rows.build();
    }

    static List<BigInteger> unscaledValues(long... values)
    {
        return Arrays.stream(values)
                .mapToObj(BigInteger::valueOf)
                .collect(toImmutableList());
    }

    record DecimalRow(Long partitionKey, Long orderKey, BigInteger unscaledValue) {}

    public static DecimalRow decimalRow(Long partitionKey, Long orderKey, BigInteger unscaledValue)
    {
        return new DecimalRow(partitionKey, orderKey, unscaledValue);
    }

    static Stream<Arguments> cartesianProduct(List<?>... dimensions)
    {
        return Lists.cartesianProduct(Arrays.asList(dimensions)).stream()
                .map(values -> Arguments.of(values.toArray()));
    }
}
