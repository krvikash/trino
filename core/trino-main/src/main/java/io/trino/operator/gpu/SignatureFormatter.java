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
package io.trino.operator.gpu;

import io.trino.execution.TableInfo;
import io.trino.metadata.IndexHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionFormatter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.planprinter.Anonymizer;
import io.trino.sql.planner.planprinter.PlanPrinter;

import java.util.Optional;

/// Formats expressions and aggregations as type-only signatures (e.g. {@code $operator$add(bigint, bigint)},
/// {@code sum(bigint)}) for GPU plan ineligibility diagnostics, stripping column names and literal values.
public final class SignatureFormatter
{
    private SignatureFormatter() {}

    private static final Anonymizer ANONYMIZER = new SignatureFormattingAnonymizer();

    public static String formatAggregation(Aggregation aggregation)
    {
        return PlanPrinter.formatAggregation(ANONYMIZER, aggregation);
    }

    public static String formatExpression(Expression expression)
    {
        return ANONYMIZER.anonymize(expression);
    }

    private static class SignatureFormattingAnonymizer
            implements Anonymizer
    {
        private static final ExpressionFormatter.Formatter EXPRESSION_FORMATTER =
                new ExpressionFormatter.Formatter(
                        Optional.of(SignatureFormattingAnonymizer::anonymizeLiteral),
                        Optional.of(SignatureFormattingAnonymizer::anonymizeSymbolReference));
        // Fallback for Anonymizer methods not exercised today; returns empty to degrade gracefully if new callers appear.
        private static final String UNKNOWN = "";

        @Override
        public String anonymize(Type type, String value)
        {
            return type.getDisplayName();
        }

        @Override
        public String anonymize(Symbol symbol)
        {
            return symbol.type().getDisplayName();
        }

        @Override
        public String anonymizeColumn(String column)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(Expression expression)
        {
            return EXPRESSION_FORMATTER.process(expression);
        }

        @Override
        public String anonymize(ColumnHandle columnHandle)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(QualifiedObjectName objectName)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(Partitioning.ArgumentBinding argument)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(IndexHandle indexHandle)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(TableHandle tableHandle, TableInfo tableInfo)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(PartitioningHandle partitioningHandle)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(TableWriterNode.WriterTarget writerTarget)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(StatisticsWriterNode.WriteStatisticsTarget writerTarget)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(TableHandle tableHandle)
        {
            return UNKNOWN;
        }

        @Override
        public String anonymize(TableExecuteHandle tableHandle)
        {
            return UNKNOWN;
        }

        private static String anonymizeSymbolReference(Reference node)
        {
            return node.type().getDisplayName();
        }

        private static String anonymizeLiteral(Constant literal)
        {
            return literal.type().getDisplayName();
        }
    }
}
