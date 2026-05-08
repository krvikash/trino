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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ast.BinaryOperator;
import ai.rapids.cudf.ast.Literal;
import ai.rapids.cudf.ast.UnaryOperator;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

/**
 * Translates a Trino join filter into a {@link CudfAstExpression}.
 */
public final class GpuJoinFilterCompiler
{
    private GpuJoinFilterCompiler() {}

    public static Optional<CudfAstExpression> compile(Expression filter)
    {
        return translate(filter);
    }

    private static Optional<CudfAstExpression> translate(Expression expression)
    {
        return switch (expression) {
            case Comparison comparison -> translateComparison(comparison);
            case Logical logical -> translateLogical(logical);
            case IsNull isNull -> translateIsNull(isNull);
            case Constant constant -> translateConstant(constant);
            case Call call -> translateCall(call);
            case Reference reference -> Optional.of(new CudfAstExpression.Reference(Symbol.from(reference)));
            default -> Optional.empty();
        };
    }

    private static Optional<CudfAstExpression> translateComparison(Comparison comparison)
    {
        return mapComparisonOperator(comparison.operator()).flatMap(operator ->
                translate(comparison.left()).flatMap(left ->
                        translate(comparison.right()).map(right ->
                                new CudfAstExpression.BinaryOperation(operator, left, right))));
    }

    private static Optional<BinaryOperator> mapComparisonOperator(Comparison.Operator op)
    {
        return switch (op) {
            case EQUAL -> Optional.of(BinaryOperator.EQUAL);
            case NOT_EQUAL -> Optional.of(BinaryOperator.NOT_EQUAL);
            case LESS_THAN -> Optional.of(BinaryOperator.LESS);
            case LESS_THAN_OR_EQUAL -> Optional.of(BinaryOperator.LESS_EQUAL);
            case GREATER_THAN -> Optional.of(BinaryOperator.GREATER);
            case GREATER_THAN_OR_EQUAL -> Optional.of(BinaryOperator.GREATER_EQUAL);
            case IDENTICAL -> Optional.empty();
        };
    }

    private static Optional<CudfAstExpression> translateLogical(Logical logical)
    {
        List<Expression> terms = logical.terms();
        return switch (terms.size()) {
            case 0 -> {
                boolean constant = switch (logical.operator()) {
                    case AND -> true;
                    case OR -> false;
                };
                yield translateConstant(new Constant(BOOLEAN, constant));
            }
            case 1 -> translate(getOnlyElement(terms));
            default -> {
                BinaryOperator op = switch (logical.operator()) {
                    case AND -> BinaryOperator.LOGICAL_AND;
                    case OR -> BinaryOperator.LOGICAL_OR;
                };
                ArrayDeque<Expression> queue = new ArrayDeque<>(terms);
                Optional<CudfAstExpression> first = translate(queue.removeFirst());
                if (first.isEmpty()) {
                    yield Optional.empty();
                }
                CudfAstExpression translated = first.get();
                while (!queue.isEmpty()) {
                    Optional<CudfAstExpression> next = translate(queue.removeFirst());
                    if (next.isEmpty()) {
                        yield Optional.empty();
                    }
                    // TODO build a balanced tree instead of left-deep
                    translated = new CudfAstExpression.BinaryOperation(op, translated, next.get());
                }
                yield Optional.of(translated);
            }
        };
    }

    private static Optional<CudfAstExpression> translateIsNull(IsNull isNull)
    {
        return translate(isNull.value()).map(value ->
                new CudfAstExpression.UnaryOperation(UnaryOperator.IS_NULL, value));
    }

    private static Optional<CudfAstExpression> translateConstant(Constant constant)
    {
        @Nullable Object value = constant.value();
        return switch (constant.type()) {
            case BooleanType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofBoolean((Boolean) value)));
            case TinyintType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofByte(value == null ? null : SignedBytes.checkedCast((long) value))));
            case SmallintType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofShort(value == null ? null : Shorts.checkedCast((long) value))));
            case IntegerType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofInt(value == null ? null : toIntExact((long) value))));
            case BigintType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofLong((Long) value)));
            case RealType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofFloat(value == null ? null : intBitsToFloat(toIntExact((long) value)))));
            case DoubleType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofDouble((Double) value)));
            case DateType _ -> Optional.of(new CudfAstExpression.Constant(Literal.ofTimestampDaysFromInt(value == null ? null : toIntExact((long) value))));
            default -> Optional.empty();
        };
    }

    /**
     * Translate a {@link Call}. Today only the boolean {@code $not} builtin is handled
     * natively (mapped to AST {@code NOT}); all other calls return empty so the planner
     * falls back to the CPU lookup-join.
     */
    private static Optional<CudfAstExpression> translateCall(Call call)
    {
        CatalogSchemaFunctionName functionName = call.function().signature().getName();
        if (!isBuiltinFunctionName(functionName)) {
            return Optional.empty();
        }
        String name = functionName.functionName();

        if (name.equals("$not") && call.arguments().size() == 1) {
            return translate(getOnlyElement(call.arguments())).map(value ->
                    new CudfAstExpression.UnaryOperation(UnaryOperator.NOT, value));
        }
        return Optional.empty();
    }
}
