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
package io.trino.sql.dialect.trino.operation;

public abstract class TrinoOperationVisitor<R, C>
{
    protected abstract R visitOperation(TrinoOperation operation, C context);

    public R visitAggregation(Aggregation operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitArray(Array operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitBetween(Between operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitBind(Bind operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitCall(Call operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitCase(Case operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitCast(Cast operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitCoalesce(Coalesce operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitComparison(Comparison operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitConstant(Constant operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitCorrelatedJoin(CorrelatedJoin operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitExchange(Exchange operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitExplainAnalyze(ExplainAnalyze operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitFieldReference(FieldReference operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitFilter(Filter operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitIn(In operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitIsNull(IsNull operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitJoin(Join operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitLambda(Lambda operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitLimit(Limit operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitLogical(Logical operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitNullIf(NullIf operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitOutput(Output operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitProject(Project operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitReturn(Return operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitRow(Row operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitSwitch(Switch operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitTableScan(TableScan operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitTopN(TopN operation, C context)
    {
        return visitOperation(operation, context);
    }

    public R visitValues(Values operation, C context)
    {
        return visitOperation(operation, context);
    }
}
