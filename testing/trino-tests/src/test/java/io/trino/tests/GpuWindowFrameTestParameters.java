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

import com.google.common.collect.Lists;
import io.trino.spi.connector.SortOrder;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class GpuWindowFrameTestParameters
{
    private GpuWindowFrameTestParameters() {}

    /// Aggregations, whose results depend on the frame
    static final List<String> AGGREGATIONS = List.of(
            "avg(argument_decimal_28_2)",
            "avg(argument_decimal_38_6)");

    /// Aggregations that the GPU does not support
    static final List<String> GPU_UNSUPPORTED_AGGREGATIONS = List.of(
            "sum(argument_decimal_38_6)",
            "avg(CAST(argument_decimal_38_6 AS bigint))",
            "avg(CAST(argument_decimal_38_6 AS real))",
            "avg(CAST(argument_decimal_38_6 AS double))");

    static final List<String> ORDERINGS = Arrays.stream(SortOrder.values()).map(SortOrder::toString).toList();

    static Stream<Arguments> cartesianProduct(List<?>... dimensions)
    {
        return Lists.cartesianProduct(Arrays.asList(dimensions)).stream()
                .map(values -> Arguments.of(values.toArray()));
    }
}
