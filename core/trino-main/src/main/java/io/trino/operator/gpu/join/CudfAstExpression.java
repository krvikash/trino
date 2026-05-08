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

import ai.rapids.cudf.ast.AstExpression;
import ai.rapids.cudf.ast.BinaryOperator;
import ai.rapids.cudf.ast.ColumnReference;
import ai.rapids.cudf.ast.Literal;
import ai.rapids.cudf.ast.TableReference;
import ai.rapids.cudf.ast.UnaryOperator;
import io.trino.sql.planner.Symbol;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Layout-agnostic mirror of {@link AstExpression}.
 */
public sealed interface CudfAstExpression
{
    AstExpression toCudfAst(Map<Symbol, Integer> leftLayout, Map<Symbol, Integer> rightLayout);

    record Reference(Symbol symbol)
            implements CudfAstExpression
    {
        public Reference
        {
            requireNonNull(symbol, "symbol is null");
        }

        @Override
        public AstExpression toCudfAst(Map<Symbol, Integer> leftLayout, Map<Symbol, Integer> rightLayout)
        {
            Integer leftChannel = leftLayout.get(symbol);
            Integer rightChannel = rightLayout.get(symbol);
            if (leftChannel != null && rightChannel != null) {
                throw new IllegalStateException("Symbol is ambiguous in join filter context: " + symbol);
            }
            if (leftChannel != null) {
                return new ColumnReference(leftChannel, TableReference.LEFT);
            }
            if (rightChannel != null) {
                return new ColumnReference(rightChannel, TableReference.RIGHT);
            }
            throw new IllegalStateException("Could not resolve symbol in join filter context: " + symbol);
        }
    }

    record BinaryOperation(BinaryOperator operator, CudfAstExpression left, CudfAstExpression right)
            implements CudfAstExpression
    {
        public BinaryOperation
        {
            requireNonNull(operator, "operator is null");
            requireNonNull(left, "left is null");
            requireNonNull(right, "right is null");
        }

        @Override
        public AstExpression toCudfAst(Map<Symbol, Integer> leftLayout, Map<Symbol, Integer> rightLayout)
        {
            return new ai.rapids.cudf.ast.BinaryOperation(operator, left.toCudfAst(leftLayout, rightLayout), right.toCudfAst(leftLayout, rightLayout));
        }
    }

    record UnaryOperation(UnaryOperator operator, CudfAstExpression source)
            implements CudfAstExpression
    {
        public UnaryOperation
        {
            requireNonNull(operator, "operator is null");
            requireNonNull(source, "source is null");
        }

        @Override
        public AstExpression toCudfAst(Map<Symbol, Integer> leftLayout, Map<Symbol, Integer> rightLayout)
        {
            return new ai.rapids.cudf.ast.UnaryOperation(operator, source.toCudfAst(leftLayout, rightLayout));
        }
    }

    record Constant(Literal value)
            implements CudfAstExpression
    {
        public Constant
        {
            requireNonNull(value, "value is null");
        }

        @Override
        public AstExpression toCudfAst(Map<Symbol, Integer> leftLayout, Map<Symbol, Integer> rightLayout)
        {
            return value;
        }
    }
}
