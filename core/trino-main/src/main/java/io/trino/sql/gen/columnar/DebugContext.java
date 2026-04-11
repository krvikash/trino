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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TypeUtils.readNativeValue;

class DebugContext
{
    private static final Logger log = Logger.get(DebugContext.class);

    private final List<Optional<Type>> inputTypes;
    private final List<Type> outputTypes;
    private final List<String> expressions;
    private final String filterExpression;
    private final boolean isDebugOutputEnabled;

    public DebugContext(List<Expression> expressions, Map<Symbol, Integer> layout, String filterExpression, boolean isDebugOutputEnabled)
    {
        this.inputTypes = expressions.stream()
                .map(expression -> {
                    List<Type> types = getInputTypes(expression, layout);
                    if (types.size() == 1) {
                        return Optional.of(types.getFirst());
                    }
                    return Optional.<Type>empty();
                })
                .collect(toImmutableList());
        this.outputTypes = expressions.stream()
                .map(Expression::type)
                .collect(toImmutableList());
        this.expressions = expressions.stream()
                .map(Expression::toString)
                .collect(toImmutableList());
        this.filterExpression = filterExpression;
        this.isDebugOutputEnabled = isDebugOutputEnabled;
    }

    public void logDebugOutput(int argument, Block output, SourcePage inputPage, SelectedPositions activePositions)
    {
        if (!isDebugOutputEnabled) {
            return;
        }
        int channels = inputPage.getChannelCount();
        if (channels > 1) {
            // Restrict debug output to single channel projections for simplicity
            return;
        }
        Block input = inputPage.getBlock(0);
        Type inputType = inputTypes.get(argument).orElseThrow();
        Type outputType = outputTypes.get(argument);
        log.info("evaluated input block: %s, activePositions %s, expression %s", input, activePositions, expressions.get(argument));
        if (activePositions.isList()) {
            int[] positions = activePositions.getPositions();
            for (int i = activePositions.getOffset(); i < activePositions.getOffset() + activePositions.size(); i++) {
                int position = positions[i];
                logDebugOutput(position, inputType, outputType, input, output);
            }
        }
        else {
            for (int i = 0; i < activePositions.size(); i++) {
                int position = activePositions.getOffset() + i;
                logDebugOutput(position, inputType, outputType, input, output);
            }
        }
    }

    private static List<Type> getInputTypes(Expression expression, Map<Symbol, Integer> layout)
    {
        TreeMap<Integer, Type> channels = new TreeMap<>();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitReference(Reference node, Void context)
            {
                Integer channel = layout.get(Symbol.from(node));
                if (channel != null) {
                    channels.computeIfAbsent(channel, _ -> node.type());
                }
                return null;
            }
        }.process(expression, null);
        return ImmutableList.copyOf(channels.values());
    }

    private static String getStringValue(Object value)
    {
        if (value == null) {
            return "null";
        }
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        return value.toString();
    }

    private static void logDebugOutput(int position, Type inputType, Type outputType, Block input, Block output)
    {
        Object inputValue = readNativeValue(inputType, input, position);
        Object outputValue = readNativeValue(outputType, output, position);
        log.info(
                "evaluated position %s: inputValue=%s string=%s, outputValue=%s string=%s",
                position,
                inputValue,
                getStringValue(inputValue),
                outputValue,
                getStringValue(outputValue));
    }

    public void logDebugFilteredPositions(SelectedPositions positions)
    {
        if (isDebugOutputEnabled) {
            log.info("filter expression: %s, positions: %s", filterExpression, positions);
        }
    }
}
