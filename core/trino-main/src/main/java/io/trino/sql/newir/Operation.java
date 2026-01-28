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
package io.trino.sql.newir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.ir.IrDialect.FunctionType;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.FormatOptions.PrintOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.newir.Dialect.validateDialectName;
import static io.trino.sql.newir.FormatOptions.INDENT;
import static io.trino.sql.newir.FormatOptions.TESTING_PRINT_OPTIONS;
import static io.trino.sql.newir.FormatValidation.isValidAttributeName;
import static io.trino.sql.newir.FormatValidation.isValidIdentifier;
import static io.trino.sql.newir.Value.validateValueName;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.partitioningBy;

/**
 * Operation is the main building block of a program.
 * Operation, as well as other code elements, does not use JSON serialization.
 * The serialized format is obtained through the print() method.
 */
public abstract non-sealed class Operation
        implements SourceNode
{
    private final String dialect;
    private final String name;

    public Operation(String dialect, String name)
    {
        requireNonNull(dialect, "dialect is null");
        validateDialectName(dialect);
        this.dialect = dialect;

        requireNonNull(name, "name is null");
        if (!isValidIdentifier(name)) {
            throw new TrinoException(IR_ERROR, format("invalid operation name: \"%s\"", name));
        }
        this.name = name;
    }

    public record Result(String name, Type type)
            implements Value
    {
        public Result
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
            validateValueName(name);
        }
    }

    public record AttributeKey(String dialect, String name)
    {
        public AttributeKey
        {
            requireNonNull(dialect, "dialect is null");
            requireNonNull(name, "name is null");
            validateDialectName(dialect);
            if (!isValidAttributeName(name)) {
                throw new TrinoException(IR_ERROR, format("invalid attribute name: \"%s\"", name));
            }
        }
    }

    public record OperationId(String name, List<Type> argumentTypes, List<Type> regionTypes)
    {
        public OperationId
        {
            requireNonNull(name, "name is null");
            requireNonNull(argumentTypes, "argumentTypes is null");
            requireNonNull(regionTypes, "regionTypes is null");

            if (!isValidIdentifier(name)) {
                throw new TrinoException(IR_ERROR, format("invalid operation name: \"%s\"", name));
            }
            if (!regionTypes.stream()
                    .allMatch(regionType -> regionType.dialect().equals(IR) && regionType.dialectType() instanceof FunctionType)) {
                throw new TrinoException(IR_ERROR, "all region types must be FunctionType");
            }
            argumentTypes = ImmutableList.copyOf(argumentTypes);
            regionTypes = ImmutableList.copyOf(regionTypes);
        }

        public FunctionType getRegionType(int index)
        {
            Type type = regionTypes.get(index);
            return (FunctionType) type.dialectType();
        }
    }

    public final String dialect()
    {
        return dialect;
    }

    public final String name()
    {
        return name;
    }

    /**
     * Return the {@link Result} produced by this operation.
     */
    public abstract Result result();

    /**
     * Return the list of arguments passed to this operation.
     * Each passed {@link Value} is either a {@link Result} of another operation or a {@link Parameter} of an enclosing block.
     */
    public abstract List<Value> arguments();

    /**
     * Return a list of {@link Region} contained in this operation.
     * Each Region is a logically independent unit of code which can be invoked by the operation.
     */
    public abstract List<Region> regions();

    /**
     * Return a map of operation's attributes.
     * Each entry represents a constant property of the operation.
     */
    public abstract Map<AttributeKey, Object> attributes();

    /**
     * Return the id of this operation, consisting of its name, argument types and region types. Regions are seen as lambdas.
     * The id must uniquely identify the operation within its dialect.
     */
    public final OperationId id()
    {
        List<Type> argumentTypes = arguments().stream()
                .map(Value::type)
                .collect(toImmutableList());
        List<Type> regionTypes = regions().stream()
                .map(Region::getFunctionType)
                .collect(toImmutableList());
        return new OperationId(name(), argumentTypes, regionTypes);
    }

    /**
     * Return the set of all value names declared and used in this operation and its nested blocks.
     */
    public Set<String> getAllValueNames()
    {
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        names.add(result().name());
        arguments().stream()
                .map(Value::name)
                .forEach(names::add);
        regions().stream()
                .map(Region::getOnlyBlock)
                .forEach(block -> {
                    block.parameters().stream()
                            .map(Value::name)
                            .forEach(names::add);
                    block.operations()
                            .forEach(nestedOperation -> names.addAll(nestedOperation.getAllValueNames()));
                });

        return names.build();
    }

    public final String print(int indentLevel, PrintOptions printOptions)
    {
        StringBuilder builder = new StringBuilder();
        String indent = INDENT.repeat(indentLevel);

        builder.append(indent)
                .append(result().name())
                .append(" = ")
                .append(printOptions.formatName(this))
                .append(arguments().stream()
                        .map(Value::name)
                        .collect(joining(", ", "(", ")")))
                .append(" : ")
                .append(arguments().stream()
                        .map(Value::type)
                        .map(printOptions::formatType)
                        .collect(joining(", ", "(", ")")))
                .append(" -> ")
                .append(printOptions.formatType(result().type()))
                .append(regions().stream()
                        .map(region -> region.print(indentLevel + 1, printOptions))
                        .collect(joining(", ", " (", ")")));

        // do not render empty attributes list
        if (!attributes().isEmpty()) {
            builder.append("\n")
                    .append(indent)
                    .append(INDENT);
            Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes().entrySet().stream()
                    .collect(partitioningBy(entry -> entry.getKey().dialect().equals(this.dialect())));
            // first output attributes from this operation's dialect in their actual order
            List<Map.Entry<AttributeKey, Object>> printOrder = new ArrayList<>(partitionedAttributes.get(true));
            // then output all other attributes sorted by dialect and name
            partitionedAttributes.get(false).stream()
                    .sorted(comparing(entry -> entry.getKey().dialect() + "." + entry.getKey().name()))
                    .forEach(printOrder::add);
            builder.append(printOrder.stream()
                    .map(entry -> printOptions.formatAttribute(entry.getKey(), entry.getValue()))
                    .collect(joining(", ", "{", "}")));
        }

        return builder.toString();
    }

    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return print(indentLevel, printOptions);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        Operation operation = (Operation) o;
        return Objects.equals(dialect, operation.dialect) &&
                Objects.equals(name, operation.name) &&
                Objects.equals(result(), operation.result()) &&
                Objects.equals(arguments(), operation.arguments()) &&
                Objects.equals(regions(), operation.regions()) &&
                Objects.equals(attributes(), operation.attributes());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dialect, name, result(), arguments(), regions(), attributes());
    }

    @Override
    public String toString()
    {
        return print(0, TESTING_PRINT_OPTIONS);
    }
}
