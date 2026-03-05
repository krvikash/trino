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
package io.trino.sql.dialect.ir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class IrDialect
        extends Dialect
{
    public static final String IR = "ir";
    public static final IrDialect IR_DIALECT = new IrDialect();

    // terminal operation ends the flow of control in a Block
    public static final String TERMINAL = "terminal";

    // location in the source code
    public static final String LOCATION = "location";

    // repeatability of the operation results to guide optimizations such as inlining, re-use, constant folding
    public static final String REPEATABILITY = "repeatability";

    // whether the operation is safe to execute (never fails)
    public static final String SAFE = "safe";

    // whether the operation has side effects
    public static final String HAS_SIDE_EFFECTS = "has_side_effects";

    public static final Map<AttributeKey, Object> DEFAULT_BLOCK_PARAMETER_ATTRIBUTES = ImmutableMap.<AttributeKey, Object>builder()
            .put(new AttributeKey(IR, REPEATABILITY), DETERMINISTIC)
            .put(new AttributeKey(IR, SAFE), TRUE)
            .put(new AttributeKey(IR, HAS_SIDE_EFFECTS), FALSE)
            .buildOrThrow();

    private IrDialect()
    {
        super(IR);
    }

    @Override
    public String formatAttribute(String name, Object attribute)
    {
        return switch (name) {
            case TERMINAL, SAFE -> {
                if (!TRUE.equals(attribute)) {
                    throw new TrinoException(IR_ERROR, format("the required value for ir.%s attribute is true. Actual: %s", name, attribute));
                }
                yield "true";
            }
            case LOCATION -> {
                if (!(attribute instanceof Location location)) {
                    throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be of type Location. Actual: %s", name, attribute.getClass().getSimpleName()));
                }
                yield location.toString();
            }
            case REPEATABILITY -> {
                if (!(attribute instanceof Repeatability repeatability)) {
                    throw new TrinoException(IR_ERROR, format(
                            "the value of ir.%s attribute must be of type Repeatability: %s. Actual: %s",
                            name,
                            Arrays.stream(Repeatability.values()).map(Repeatability::name).collect(joining(", ")),
                            attribute.getClass().getSimpleName()));
                }
                yield repeatability.name();
            }
            case HAS_SIDE_EFFECTS -> {
                if (!(attribute instanceof Boolean)) {
                    throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be of type Boolean. Actual: %s", name, attribute.getClass().getSimpleName()));
                }
                yield attribute.toString();
            }
            default -> throw new TrinoException(IR_ERROR, format("the ir dialect does not support attribute %s", name));
        };
    }

    @Override
    public Object parseAttribute(String name, String attribute)
    {
        return switch (name) {
            case TERMINAL, SAFE -> {
                if (!"true".equals(attribute)) {
                    throw new TrinoException(IR_ERROR, format("the required value for ir.%s attribute is \"true\". Actual: %s", name, attribute));
                }
                yield true;
            }
            case LOCATION -> {
                String[] parts = attribute.split(":");
                if (parts.length != 2) {
                    throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be in format \"line:column\". Actual: %s", name, attribute));
                }
                int line;
                int column;
                try {
                    line = Integer.parseInt(parts[0]);
                    column = Integer.parseInt(parts[1]);
                }
                catch (NumberFormatException e) {
                    throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be in format \"line:column\" with integer values. Actual: %s", name, attribute), e);
                }
                yield new Location(line, column);
            }
            case REPEATABILITY -> {
                try {
                    yield Repeatability.valueOf(attribute);
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(IR_ERROR, format(
                            "the value of ir.%s attribute must be one of: %s. Actual: %s",
                            name,
                            Arrays.stream(Repeatability.values()).map(Repeatability::name).collect(joining(", ")),
                            attribute));
                }
            }
            case HAS_SIDE_EFFECTS -> {
                if (!"true".equals(attribute) && !"false".equals(attribute)) {
                    throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be either \"true\" or \"false\". Actual: %s", name, attribute));
                }
                yield parseBoolean(attribute);
            }
            default -> throw new TrinoException(IR_ERROR, format("the ir dialect does not support attribute %s", name));
        };
    }

    @Override
    public String formatType(Type type)
    {
        if (!(type.dialectType() instanceof FunctionType)) {
            throw new UnsupportedOperationException("the ir dialect does not support type " + type);
        }
        throw new UnsupportedOperationException("formatType is not yet implemented for FunctionType");
    }

    @Override
    public Type parseType(String type)
    {
        throw new UnsupportedOperationException("parseType is not yet implemented");
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> getAttributeDerivationForOperation(OperationId id)
    {
        throw new UnsupportedOperationException("the ir dialect does not support any operations");
    }

    @Override
    public Set<AttributeKey> getInherentOperationAttributeKeys(OperationId id)
    {
        throw new UnsupportedOperationException("the ir dialect does not support any operations");
    }

    @Override
    public Operation createOperation(String name, String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        throw new UnsupportedOperationException("the ir dialect does not support any operations");
    }

    public enum Repeatability
    {
        // The operation always produces the same output for the same input.
        // DETERMINISTIC operations can be inlined, re-used and constant folded.
        DETERMINISTIC,

        // The operation may produce different outputs for the same input, but there are no guarantees.
        // NON_IDEMPOTENT operations should not be inlined. They can be re-used and constant folded.
        NON_IDEMPOTENT,

        // The operation shall produce different outputs for the same input on each invocation.
        // NON_DETERMINISTIC operations should not be inlined, re-used or constant folded.
        NON_DETERMINISTIC
    }

    public record FunctionType(List<Type> argumentTypes, Type returnType)
    {
        public FunctionType
        {
            argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
            requireNonNull(returnType, "returnType is null");
        }
    }
}
