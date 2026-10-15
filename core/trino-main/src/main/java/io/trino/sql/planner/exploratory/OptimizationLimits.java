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

import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * Limits the resources used by one optimization pass.
 * <p>
 * These are hard safety limits. Exceeding any limit aborts the pass with {@code XO_ERROR}; it does not stop exploration
 * early or extract the best plan found so far. Accounting uses coarse deterministic work counters rather than exact
 * instruction, CPU, or allocation counts. All work counters are cumulative for one pass, and consumed work is never
 * refunded.
 * <p>
 * Default budgets are derived from the size of the original {@link Program}. Every operation contributes one input
 * token, plus one token per argument and region. Fixed allowances protect small programs, factors allow budgets to
 * grow proportionally with the input, and caps provide emergency boundaries.
 * <p>
 * Memo growth tokens are cumulative. The initial Memo and every actually inserted operation consume tokens according
 * to operation arity; removal, merge, and pruning do not refund them. They therefore bound current Memo size
 * conservatively while also rejecting excessive structural churn. The same maximum bounds the combined shape of the
 * results produced by one rule application before deduplication. Actual inserted growth, including merge-induced
 * rewrites, is charged cumulatively after insertion.
 * <p>
 * The defaults are capped versions of:
 * <pre>
 * memoGrowthTokens = fixed + memoGrowthTokensPerInputToken * inputTokens
 * optimizationWork = fixed + workPerMemoGrowthTokenAndRule * memoGrowthTokens * (ruleCount + 1)
 * outputOperations = fixed + outputOperationsPerInputToken * inputTokens
 * extractionAttempts = extractionAttemptsPerOutputOperation * outputOperations
 * </pre>
 * <p>
 * The constants were calibrated against 250 plans from four TPCH/TPCDS suites with XO, using a four-rule set, no
 * backtracking during extraction, and no enforcers. The observed maxima were 3,594 Memo growth tokens, 804,096
 * optimization work, and 1,884 output operations. The selected limits retain 27.8x, 6.22x, and 53x headroom,
 * respectively. At the optimization-work limit, the current high-work p95 rate projects about 32 seconds of wall time;
 * the conservative upper envelope across calibration runs is 42 seconds. Resource measurements used an 8 GiB test JVM
 * and concurrent suite execution, so production results depend on coordinator size and load. Recalibrate after adding
 * retirement, backtracking, enforcers, or materially changing the rule set.
 * <p>
 * The limits scale linearly with input size, which fits the current local-rewrite rules. Join enumeration makes
 * legitimate Memo growth exponential in the join count and will require a superlinear term.
 */
public record OptimizationLimits(
        long maxMemoGrowthTokens,
        long maxOptimizationWork,
        long maxExtractionAttempts,
        long maxOutputOperations)
{
    // Allow substantial Memo growth for small inputs and future enumeration, but bound cumulative structural churn.
    private static final long FIXED_MEMO_GROWTH_TOKENS = 10_000;
    private static final long MEMO_GROWTH_TOKENS_PER_INPUT_TOKEN = 100;
    private static final long MAX_MEMO_GROWTH_TOKENS = 100_000;

    // Scale work with both Memo capacity and rule count, while bounding time spent before legacy fallback.
    private static final long FIXED_OPTIMIZATION_WORK = 100_000;
    private static final long OPTIMIZATION_WORK_PER_MEMO_GROWTH_TOKEN_AND_RULE = 10;
    private static final long MAX_OPTIMIZATION_WORK = 5_000_000;

    // Allow output to grow by an order of magnitude; the cap matches the largest admitted input-token budget.
    private static final long FIXED_OUTPUT_OPERATIONS = 1_000;
    private static final long OUTPUT_OPERATIONS_PER_INPUT_TOKEN = 10;
    private static final long MAX_OUTPUT_OPERATIONS = MAX_MEMO_GROWTH_TOKENS;

    // Reserve ten candidate attempts per output operation for extraction backtracking.
    private static final long EXTRACTION_ATTEMPTS_PER_OUTPUT_OPERATION = 10;
    private static final long MAX_EXTRACTION_ATTEMPTS = EXTRACTION_ATTEMPTS_PER_OUTPUT_OPERATION * MAX_OUTPUT_OPERATIONS;

    public OptimizationLimits
    {
        checkArgument(maxMemoGrowthTokens >= 0, "maxMemoGrowthTokens is negative");
        checkArgument(maxOptimizationWork >= 0, "maxOptimizationWork is negative");
        checkArgument(maxExtractionAttempts >= 0, "maxExtractionAttempts is negative");
        checkArgument(maxOutputOperations >= 0, "maxOutputOperations is negative");
    }

    public static OptimizationLimits defaultFor(Program program, int ruleCount)
    {
        requireNonNull(program, "program is null");
        return defaultFor(operationTokens(program.root()), ruleCount);
    }

    static OptimizationLimits defaultFor(long inputTokens, int ruleCount)
    {
        checkArgument(inputTokens >= 0, "inputTokens is negative");
        checkArgument(ruleCount >= 0, "ruleCount is negative");

        long memoGrowthTokens = scaledLimit(FIXED_MEMO_GROWTH_TOKENS, MEMO_GROWTH_TOKENS_PER_INPUT_TOKEN, inputTokens, MAX_MEMO_GROWTH_TOKENS);
        long optimizationWork = scaledLimit(
                FIXED_OPTIMIZATION_WORK,
                OPTIMIZATION_WORK_PER_MEMO_GROWTH_TOKEN_AND_RULE,
                saturatedMultiply(memoGrowthTokens, (long) ruleCount + 1),
                MAX_OPTIMIZATION_WORK);
        long outputOperations = scaledLimit(FIXED_OUTPUT_OPERATIONS, OUTPUT_OPERATIONS_PER_INPUT_TOKEN, inputTokens, MAX_OUTPUT_OPERATIONS);
        long extractionAttempts = scaledLimit(0, EXTRACTION_ATTEMPTS_PER_OUTPUT_OPERATION, outputOperations, MAX_EXTRACTION_ATTEMPTS);
        return new OptimizationLimits(memoGrowthTokens, optimizationWork, extractionAttempts, outputOperations);
    }

    static long operationTokens(MemoOperation operation)
    {
        return 1L + operation.children().size();
    }

    static long operationTokens(NewMemoOperation operation)
    {
        return 1L + operation.children().size();
    }

    private static long operationTokens(Operation operation)
    {
        long tokens = 1L + operation.arguments().size() + operation.regions().size();
        for (Region region : operation.regions()) {
            for (Block block : region.blocks()) {
                for (Operation nestedOperation : block.operations()) {
                    tokens = saturatedAdd(tokens, operationTokens(nestedOperation));
                }
            }
        }
        return tokens;
    }

    private static long scaledLimit(long fixedAllowance, long factor, long size, long maximum)
    {
        return min(maximum, saturatedAdd(fixedAllowance, saturatedMultiply(factor, size)));
    }
}
