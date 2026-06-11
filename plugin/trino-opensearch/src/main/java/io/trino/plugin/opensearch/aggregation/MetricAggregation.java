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
package io.trino.plugin.opensearch.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record MetricAggregation(String functionName, Type outputType, Optional<OpenSearchColumnHandle> columnHandle, String alias)
{
    public static final String MAX = "max";
    public static final String MIN = "min";
    public static final String AVG = "avg";
    public static final String SUM = "sum";
    public static final String COUNT = "count";
    private static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = Arrays.asList(MAX, MIN, AVG, SUM, COUNT);
    private static final List<Type> SUPPORTED_TYPES = Arrays.asList(REAL, DOUBLE, TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR);

    @JsonCreator
    public MetricAggregation
    {
        requireNonNull(functionName, "functionName is null");
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(alias, "alias is null");
        requireNonNull(outputType, "outputType is null");
    }

    public static Optional<MetricAggregation> handleAggregation(
            AggregateFunction function,
            Map<String, ColumnHandle> assignments,
            String columnName)
    {
        if (!SUPPORTED_AGGREGATION_FUNCTIONS.contains(function.getFunctionName())) {
            return Optional.empty();
        }

        if (function.getFunctionName().equalsIgnoreCase("count") && function.getArguments().isEmpty()) {
            return Optional.of(new MetricAggregation("count", function.getOutputType(), Optional.empty(), columnName));
        }
        // check
        // 1. Function input can be found in assignments
        // 2. Target type of column being aggregate must be numeric type
        // 3. ColumnHandle support predicates(since text treats as VARCHAR, but text can not be treated as term in os by default
        List<String> variables = function.getArguments().stream()
                .filter(input -> input instanceof Variable)
                .map(Variable.class::cast)
                .map(Variable::getName)
                .filter(assignments::containsKey)
                .toList();

        if (variables.size() != 1) {
            return Optional.empty();
        }

        OpenSearchColumnHandle columnHandle = (OpenSearchColumnHandle) assignments.get(variables.getFirst());
        if (isUnSupportedAggregation(function, columnHandle)) {
            return Optional.empty();
        }
        return Optional.of(new MetricAggregation(function.getFunctionName(), function.getOutputType(), Optional.of(columnHandle), columnName));
    }

    private static boolean isUnSupportedAggregation(AggregateFunction function, OpenSearchColumnHandle columnHandle)
    {
        // https://docs.opensearch.org/latest/aggregations/#limitations
        boolean isBigintAggregation = SUPPORTED_AGGREGATION_FUNCTIONS.contains(function.getFunctionName().toLowerCase(ENGLISH)) && columnHandle.type().equals(BIGINT);
        return isBigintAggregation ||
                !isSupportedType(columnHandle.type()) ||
                !columnHandle.supportsPredicates() ||
                // text fields that support predicates only via a keyword multi-field cannot be aggregated directly in OpenSearch
                columnHandle.delegatedField().isPresent();
    }

    private static boolean isSupportedType(Type type)
    {
        return SUPPORTED_TYPES.contains(type);
    }

    @Override
    public String toString()
    {
        return format("%s(%s)", functionName, columnHandle.map(OpenSearchColumnHandle::name).orElse(""));
    }
}
