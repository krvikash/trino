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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFunctionCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
    private static final Call ADD_10_EXPRESSION = call(
            FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
            new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 10L));

    @Test
    public void testFailureDoesNotCorruptFutureResults()
    {
        PageFunctionCompiler functionCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        PageProjection projection = projectionSupplier.get();

        // process good page and verify we got the expected number of result rows
        Page goodPage = createPageWithDataAtChannel2(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Block goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());

        // addition will throw due to integer overflow
        Page badPage = createPageWithDataAtChannel2(0, 1, 2, 3, 4, Long.MAX_VALUE);
        assertTrinoExceptionThrownBy(() -> project(projection, badPage, SelectedPositions.positionsRange(0, 100)))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // running the good page should still work
        // if block builder in generated code was not reset properly, we could get junk results after the failure
        goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());
    }

    @Test
    public void testProjectionCache()
    {
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Page page = createPageWithDataAtChannel2(0, 1, 2, 3);

        // First compile: cache miss → triggers class compilation
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(1);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Second compile with same expression: cache hit → no new compilation
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // classNameSuffix does not affect cache key
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.of("hint"));
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(3);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Cached projections produce correct results
        PageProjection projection = cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty()).get();
        assertThat(project(projection, page, SelectedPositions.positionsRange(0, 4)).getPositionCount()).isEqualTo(4);

        // No-cache compiler always compiles
        PageFunctionCompiler noCacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();
        assertThat(noCacheCompiler.getProjectionCache()).isNull();
    }

    @Test
    public void testProjectionCacheWithDifferentLayouts()
    {
        // The column is at position 2 in the first layout and position 3 in the second.
        // Both should reuse the same cached compiled class but bind correct InputChannels.
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Map<Symbol, Integer> layout1 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        Map<Symbol, Integer> layout2 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 3);

        PageProjection projection1 = cacheCompiler.compileProjection(ADD_10_EXPRESSION, layout1, Optional.empty()).get();
        PageProjection projection2 = cacheCompiler.compileProjection(ADD_10_EXPRESSION, layout2, Optional.empty()).get();

        // Verify cache hit: only one compilation despite two calls with different layouts
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Page with four columns: channels 0-1 are padding, channel 2 is [100, 200, 300], channel 3 is [1, 2, 3]
        SourcePage sourcePage = SourcePage.create(new Page(
                createRepeatedValuesBlock(0L, 3),
                createRepeatedValuesBlock(0L, 3),
                createLongBlockPage(100, 200, 300).getBlock(0),
                createLongBlockPage(1, 2, 3).getBlock(0)));

        // projection1 reads from source column 2 via InputChannels: expects 110, 210, 310
        SourcePage inputPage1 = projection1.getInputChannels().getInputChannels(sourcePage);
        Block result1 = projection1.project(SESSION, inputPage1, SelectedPositions.positionsRange(0, 3));
        assertThat(BIGINT.getLong(result1, 0)).isEqualTo(110);
        assertThat(BIGINT.getLong(result1, 1)).isEqualTo(210);

        // projection2 reads from source column 3 via InputChannels: expects 11, 12, 13
        SourcePage inputPage2 = projection2.getInputChannels().getInputChannels(sourcePage);
        Block result2 = projection2.project(SESSION, inputPage2, SelectedPositions.positionsRange(0, 3));
        assertThat(BIGINT.getLong(result2, 0)).isEqualTo(11);
        assertThat(BIGINT.getLong(result2, 1)).isEqualTo(12);
    }

    @Test
    public void testFilterCache()
    {
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Expression filter = new Comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);

        // First compile: cache miss
        cacheCompiler.compileFilter(filter, layout, Optional.empty());
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(1);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Second compile: cache hit
        cacheCompiler.compileFilter(filter, layout, Optional.empty());
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // classNameSuffix does not affect cache key
        cacheCompiler.compileFilter(filter, layout, Optional.of("hint"));
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(3);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Cached filter produces correct results
        Page page = createPageWithDataAtChannel2(0, 1, 2, 3, 4);
        PageFilter compiled = cacheCompiler.compileFilter(filter, layout, Optional.empty()).get();
        SourcePage inputPage = compiled.getInputChannels().getInputChannels(SourcePage.create(page));
        SelectedPositions result = compiled.filter(SESSION, inputPage);
        assertThat(result.size()).isEqualTo(2); // values > 2 at positions 3, 4
    }

    @Test
    public void testFilterCacheWithDifferentLayouts()
    {
        // Filter: $col_0 > 2, with column at different positions in each layout
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Expression filter = new Comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));

        Map<Symbol, Integer> layout1 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        Map<Symbol, Integer> layout2 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 3);

        PageFilter filter1 = cacheCompiler.compileFilter(filter, layout1, Optional.empty()).get();
        PageFilter filter2 = cacheCompiler.compileFilter(filter, layout2, Optional.empty()).get();

        // Verify cache hit: only one compilation despite two calls with different layouts
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Page with four columns: channels 0-1 are padding, channel 2 is [0, 1, 2, 3, 4], channel 3 is [10, 20, 30, 40, 50]
        SourcePage sourcePage = SourcePage.create(new Page(
                createRepeatedValuesBlock(0L, 5),
                createRepeatedValuesBlock(0L, 5),
                createLongBlockPage(0, 1, 2, 3, 4).getBlock(0),
                createLongBlockPage(10, 20, 30, 40, 50).getBlock(0)));

        // filter1 reads source column 2 via InputChannels: values > 2 are at positions 3, 4
        SourcePage inputPage1 = filter1.getInputChannels().getInputChannels(sourcePage);
        SelectedPositions result1 = filter1.filter(SESSION, inputPage1);
        assertThat(result1.size()).isEqualTo(2);

        // filter2 reads source column 3 via InputChannels: all values > 2, so all 5 positions selected
        SourcePage inputPage2 = filter2.getInputChannels().getInputChannels(sourcePage);
        SelectedPositions result2 = filter2.filter(SESSION, inputPage2);
        assertThat(result2.size()).isEqualTo(5);
    }

    @Test
    public void testHugeSearchedCase()
    {
        PageFunctionCompiler functionCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        int branchCount = 100;

        // Outer CASE around an inner CASE — exercises chunked class splitting
        // CASE
        //   WHEN 0 = (CASE WHEN x = 0 THEN 0 WHEN x = 1 THEN 10 ... ELSE -1 END) THEN 0
        //   WHEN 10 = (CASE WHEN x = 0 THEN 0 ... ELSE -1 END) THEN 1
        //   ...
        //   ELSE -1
        // END
        List<WhenClause> whenClauses = new ArrayList<>();
        for (long i = 0; i < branchCount; i++) {
            List<WhenClause> innerWhenClauses = new ArrayList<>();
            for (long j = 0; j < branchCount; j++) {
                innerWhenClauses.add(whenClause(new Reference(BIGINT, "x"), j, j * 10));
            }
            Case innerCaseWhen = new Case(innerWhenClauses, new Constant(BIGINT, -1L));
            whenClauses.add(whenClause(innerCaseWhen, i * 10, i));
        }
        Case caseWhen = new Case(whenClauses, new Constant(BIGINT, -1L));
        Map<Symbol, Integer> sourceLayout = Map.of(new Symbol(BIGINT, "x"), 0);

        Page inputPage = rangeLongBlockPage(0, branchCount);

        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(caseWhen, sourceLayout, Optional.empty());
        Block projectionResult = project(projectionSupplier.get(), inputPage, SelectedPositions.positionsRange(0, inputPage.getPositionCount()));
        for (int i = 0; i < inputPage.getPositionCount() - 1; i++) {
            assertThat(BIGINT.getLong(projectionResult, i)).isEqualTo(i);
        }
        assertThat(BIGINT.getLong(projectionResult, inputPage.getPositionCount() - 1)).isEqualTo(-1);

        Expression filterExpression = new Comparison(EQUAL, caseWhen, new Constant(BIGINT, -1L));
        Supplier<PageFilter> filterSupplier = functionCompiler.compileFilter(filterExpression, sourceLayout, Optional.empty());
        SelectedPositions filterResult = filterSupplier.get().filter(SESSION, SourcePage.create(inputPage));
        assertThat(filterResult.getPositions()).containsExactly(100);
    }

    private Block project(PageProjection projection, Page page, SelectedPositions selectedPositions)
    {
        SourcePage sourcePage = SourcePage.create(page);
        SourcePage inputPage = projection.getInputChannels().getInputChannels(sourcePage);
        return projection.project(SESSION, inputPage, selectedPositions);
    }

    private static Page createLongBlockPage(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return new Page(builder.build());
    }

    private static Page createPageWithDataAtChannel2(long... values)
    {
        Page data = createLongBlockPage(values);
        return new Page(createRepeatedValuesBlock(0L, values.length), createRepeatedValuesBlock(0L, values.length), data.getBlock(0));
    }

    private static Page rangeLongBlockPage(long startInclusive, long endInclusive)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder((int) (endInclusive - startInclusive + 1));
        for (long value = startInclusive; value <= endInclusive; value++) {
            BIGINT.writeLong(builder, value);
        }
        return new Page(builder.build());
    }

    private static WhenClause whenClause(Expression left, long right, long result)
    {
        return new WhenClause(
                new Comparison(EQUAL, left, new Constant(BIGINT, right)),
                new Constant(BIGINT, result));
    }
}
