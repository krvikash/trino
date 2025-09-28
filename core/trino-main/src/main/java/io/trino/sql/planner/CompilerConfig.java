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
package io.trino.sql.planner;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.Min;

@DefunctConfig("compiler.interpreter-enabled")
public class CompilerConfig
{
    public static final int DEFAULT_ROW_EXPRESSION_MAX_METHODS_PER_CLASS = 10;
    // This is an arbitrary value determined through experimentation. It must be
    // low enough to ensure that the generated code does not exceed the method size limit,
    // but high enough to minimize the overhead of method invocations.
    public static final int DEFAULT_ROW_EXPRESSION_MAX_METHOD_COMPLEXITY = 1000;
    private int expressionCacheSize = 10_000;
    private int rowExpressionMaxMethodComplexity = DEFAULT_ROW_EXPRESSION_MAX_METHOD_COMPLEXITY;
    private int rowExpressionMaxMethodsPerClass = DEFAULT_ROW_EXPRESSION_MAX_METHODS_PER_CLASS;
    private boolean specializeAggregationLoops = true;
    private boolean columnarFilterSubExpressionEvaluationEnabled;

    @Min(0)
    public int getExpressionCacheSize()
    {
        return expressionCacheSize;
    }

    @Config("compiler.expression-cache-size")
    @ConfigDescription("Reuse compiled expressions across multiple queries")
    public CompilerConfig setExpressionCacheSize(int expressionCacheSize)
    {
        this.expressionCacheSize = expressionCacheSize;
        return this;
    }

    public int getRowExpressionMaxMethodComplexity()
    {
        return rowExpressionMaxMethodComplexity;
    }

    @Config("compiler.row-expression-max-method-complexity")
    @ConfigDescription("Max method complexity before it is split into chunks")
    public CompilerConfig setRowExpressionMaxMethodComplexity(int rowExpressionMaxMethodComplexity)
    {
        this.rowExpressionMaxMethodComplexity = rowExpressionMaxMethodComplexity;
        return this;
    }

    public int getRowExpressionMaxMethodsPerClass()
    {
        return rowExpressionMaxMethodsPerClass;
    }

    @Config("compiler.row-expression-max-methods-per-class")
    @ConfigDescription("Max method count in the generated class before the class is split into chunks")
    public CompilerConfig setRowExpressionMaxMethodsPerClass(int rowExpressionMaxMethodsPerClass)
    {
        this.rowExpressionMaxMethodsPerClass = rowExpressionMaxMethodsPerClass;
        return this;
    }

    public boolean isSpecializeAggregationLoops()
    {
        return specializeAggregationLoops;
    }

    @Config("compiler.specialized-aggregation-loops")
    public CompilerConfig setSpecializeAggregationLoops(boolean specializeAggregationLoops)
    {
        this.specializeAggregationLoops = specializeAggregationLoops;
        return this;
    }

    public boolean isColumnarFilterSubExpressionEvaluationEnabled()
    {
        return columnarFilterSubExpressionEvaluationEnabled;
    }

    @Config("compiler.columnar-filter-sub-expression-evaluation.enabled")
    @LegacyConfig("debug.cast-filter-evaluation.enabled")
    @ConfigDescription("Enables columnar evaluation of filter sub-expressions")
    public CompilerConfig setColumnarFilterSubExpressionEvaluationEnabled(boolean columnarFilterSubExpressionEvaluationEnabled)
    {
        this.columnarFilterSubExpressionEvaluationEnabled = columnarFilterSubExpressionEvaluationEnabled;
        return this;
    }
}
