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
package io.trino.sql.planner.exploratory;

import io.trino.spi.TrinoException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.XO_ERROR;
import static java.util.Objects.requireNonNull;

final class OptimizationBudget
{
    private final OptimizationLimits limits;
    private long remainingMemoGrowthTokens;
    private long remainingOptimizationWork;
    private long remainingExtractionAttempts;
    private long remainingOutputOperations;

    public OptimizationBudget(OptimizationLimits limits)
    {
        this.limits = requireNonNull(limits, "limits is null");
        remainingMemoGrowthTokens = limits.maxMemoGrowthTokens();
        remainingOptimizationWork = limits.maxOptimizationWork();
        remainingExtractionAttempts = limits.maxExtractionAttempts();
        remainingOutputOperations = limits.maxOutputOperations();
    }

    public void consumeMemoGrowthTokens(long tokens)
    {
        remainingMemoGrowthTokens = consume("Memo growth tokens", tokens, remainingMemoGrowthTokens, limits.maxMemoGrowthTokens());
    }

    public void checkRuleResultWithinMemoGrowthLimit(long tokens)
    {
        checkArgument(tokens >= 0, "tokens is negative");
        if (tokens > limits.maxMemoGrowthTokens()) {
            throw new TrinoException(
                    XO_ERROR,
                    "Memo growth tokens limit exceeded by one rule application: limit %s, requested %s"
                            .formatted(limits.maxMemoGrowthTokens(), tokens));
        }
    }

    public long consumedMemoGrowthTokens()
    {
        return limits.maxMemoGrowthTokens() - remainingMemoGrowthTokens;
    }

    public long consumedOptimizationWork()
    {
        return limits.maxOptimizationWork() - remainingOptimizationWork;
    }

    public void consumeOptimizationWork(long amount)
    {
        remainingOptimizationWork = consume("Optimization work", amount, remainingOptimizationWork, limits.maxOptimizationWork());
    }

    public void consumeExtractionAttempt()
    {
        remainingExtractionAttempts = consume("Extraction attempts", 1, remainingExtractionAttempts, limits.maxExtractionAttempts());
    }

    public void consumeOutputOperation()
    {
        remainingOutputOperations = consume("Output operations", 1, remainingOutputOperations, limits.maxOutputOperations());
    }

    private static long consume(String name, long amount, long remaining, long limit)
    {
        checkArgument(amount >= 0, "amount is negative");
        if (amount > remaining) {
            throw new TrinoException(
                    XO_ERROR,
                    "%s limit exceeded: limit %s, consumed %s, requested %s"
                            .formatted(name, limit, limit - remaining, amount));
        }
        return remaining - amount;
    }
}
