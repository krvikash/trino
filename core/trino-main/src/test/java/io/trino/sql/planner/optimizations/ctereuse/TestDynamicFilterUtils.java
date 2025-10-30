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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.DynamicFilters;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.DynamicFilterExtractionResult;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.OR;
import static io.trino.sql.planner.optimizations.ctereuse.ComparatorIgnoringDerivedAttributes.extractionResultComparatorIgnoringDerivedAttributes;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.extractDynamicConjunct;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.extractDynamicFilters;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.getDynamicFilterId;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.isDynamicFilter;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.isDynamicFilterFunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDynamicFilterUtils
{
    private static final Type RELATION_ROW_TYPE = anonymousRow(BIGINT, BOOLEAN, VARCHAR);
    private static final Block.Parameter RELATION_ROW_PARAMETER = new Block.Parameter("%relationRowParameter", irType(RELATION_ROW_TYPE));
    private static final ResolvedFunction DYNAMIC_FILTER_FUNCTION = new TestingFunctionResolution().resolveFunction(DynamicFilters.Function.NAME, fromTypes(BOOLEAN, VARCHAR, VARCHAR, BOOLEAN, INTEGER));
    private static final ResolvedFunction DYNAMIC_FILTER_NULLABLE_FUNCTION = new TestingFunctionResolution().resolveFunction(DynamicFilters.NullableFunction.NAME, fromTypes(VARCHAR, VARCHAR, VARCHAR, BOOLEAN, INTEGER));

    private static final Block SINGLE_DYNAMIC_FILTER = getSingleDynamicFilter();
    private static final Block DYNAMIC_FILTERS_CONJUNCTION = getDynamicFiltersConjunction();
    private static final Block DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION = getDynamicAndStaticFiltersConjunction();
    private static final Block NESTED_DYNAMIC_FILTER = getNestedDynamicFilter();
    private static final Block DYNAMIC_FILTER_DISJUNCTION = getDynamicFilterDisjunction();
    private static final Block STATIC_FILTER = getStaticFilter();

    @Test
    public void testIsDynamicFilter()
    {
        assertThat(isDynamicFilter(SINGLE_DYNAMIC_FILTER)).isTrue();
        assertThat(isDynamicFilter(DYNAMIC_FILTERS_CONJUNCTION)).isFalse();
        assertThat(isDynamicFilter(DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION)).isFalse();
        assertThat(isDynamicFilter(NESTED_DYNAMIC_FILTER)).isFalse();
        assertThat(isDynamicFilter(DYNAMIC_FILTER_DISJUNCTION)).isFalse();
        assertThat(isDynamicFilter(STATIC_FILTER)).isFalse();
    }

    @Test
    public void testIsDynamicFilterFunction()
    {
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(fieldReference.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(fieldReference.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));

        assertThat(isDynamicFilterFunction(dynamicFilterCall)).isTrue();
    }

    @Test
    public void testGetDynamicFilterId()
    {
        assertThat(getDynamicFilterId(SINGLE_DYNAMIC_FILTER))
                .isEqualTo("df_0");

        assertThatThrownBy(() -> getDynamicFilterId(DYNAMIC_FILTERS_CONJUNCTION))
                .hasMessage("expected dynamic filter");
    }

    @Test
    public void testExtractDynamicFilters()
    {
        // the resulting blocks have the same name and parameters as the input block
        assertThat(extractDynamicFilters(SINGLE_DYNAMIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new DynamicFilterExtractionResult(
                        SINGLE_DYNAMIC_FILTER,
                        truePredicate(SINGLE_DYNAMIC_FILTER.name(), SINGLE_DYNAMIC_FILTER.parameters(), new ProgramBuilder.ValueNameAllocator(100))));

        assertThat(extractDynamicFilters(DYNAMIC_FILTERS_CONJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(extractionResultComparatorIgnoringDerivedAttributes())
                .isEqualTo(new DynamicFilterExtractionResult(
                        getRemappedDynamicFiltersConjunction(),
                        truePredicate(DYNAMIC_FILTERS_CONJUNCTION.name(), DYNAMIC_FILTERS_CONJUNCTION.parameters(), new ProgramBuilder.ValueNameAllocator(118))));

        assertThat(extractDynamicFilters(DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(extractionResultComparatorIgnoringDerivedAttributes())
                .isEqualTo(new DynamicFilterExtractionResult(
                        getRemappedDynamicFilterConjunct(),
                        getRemappedStaticFilterConjunct()));

        assertThatThrownBy(() -> extractDynamicFilters(NESTED_DYNAMIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected block returning boolean");

        assertThatThrownBy(() -> extractDynamicFilters(DYNAMIC_FILTER_DISJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("unexpected dynamic filter");

        assertThat(extractDynamicFilters(STATIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new DynamicFilterExtractionResult(
                        truePredicate(STATIC_FILTER.name(), STATIC_FILTER.parameters(), new ProgramBuilder.ValueNameAllocator(100)),
                        STATIC_FILTER));
    }

    // same conjunction as in DYNAMIC_FILTERS_CONJUNCTION, but with all values remapped due to `PredicateUtils.logical()`
    private static Block getRemappedDynamicFiltersConjunction()
    {
        // first dynamic filter
        FieldReference field1 = new FieldReference("%102", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator1 = new Constant("%103", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId1 = new Constant("%104", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed1 = new Constant("%105", BOOLEAN, false);
        Constant timeout1 = new Constant("%106", INTEGER, null);
        Call dynamicFilterCall1 = new Call(
                "%107",
                ImmutableList.of(field1.result(), comparisonOperator1.result(), dynamicFilterId1.result(), nullAllowed1.result(), timeout1.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(field1.attributes(), comparisonOperator1.attributes(), dynamicFilterId1.attributes(), nullAllowed1.attributes(), timeout1.attributes()));

        // second dynamic filter
        FieldReference field2 = new FieldReference("%109", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator2 = new Constant("%110", VARCHAR, utf8Slice("<="));
        Constant dynamicFilterId2 = new Constant("%111", VARCHAR, utf8Slice("df_1"));
        Constant nullAllowed2 = new Constant("%112", BOOLEAN, true);
        Constant timeout2 = new Constant("%113", INTEGER, 5L);
        Call dynamicFilterCall2 = new Call(
                "%114",
                ImmutableList.of(field2.result(), comparisonOperator2.result(), dynamicFilterId2.result(), nullAllowed2.result(), timeout2.result()),
                DYNAMIC_FILTER_NULLABLE_FUNCTION,
                ImmutableList.of(field2.attributes(), comparisonOperator2.attributes(), dynamicFilterId2.attributes(), nullAllowed2.attributes(), timeout2.attributes()));

        Logical conjunction = new Logical("%116", ImmutableList.of(dynamicFilterCall1.result(), dynamicFilterCall2.result()), AND, ImmutableList.of(dynamicFilterCall1.attributes(), dynamicFilterCall2.attributes()));
        Return returnOperation = new Return("%117", conjunction.result(), conjunction.attributes());

        return new Block(
                Optional.of("^dynamicFiltersConjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(
                        field1,
                        comparisonOperator1,
                        dynamicFilterId1,
                        nullAllowed1,
                        timeout1,
                        dynamicFilterCall1,
                        field2,
                        comparisonOperator2,
                        dynamicFilterId2,
                        nullAllowed2,
                        timeout2,
                        dynamicFilterCall2,
                        conjunction,
                        returnOperation));
    }

    // the dynamic conjunct from DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION, but with the return value remapped due to `PredicateUtils.extractLogicalTerms()`
    private static Block getRemappedDynamicFilterConjunct()
    {
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(fieldReference.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(fieldReference.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));
        Return returnOperation = new Return("%100", dynamicFilterCall.result(), dynamicFilterCall.attributes());

        return new Block(
                Optional.of("^dynamicAndStaticFiltersConjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, returnOperation));
    }

    // the static conjunct from DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION, but with the return value remapped due to `PredicateUtils.extractLogicalTerms()`
    private static Block getRemappedStaticFilterConjunct()
    {
        FieldReference fieldReference = new FieldReference("%6", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%101", fieldReference.result(), fieldReference.attributes());

        return new Block(
                Optional.of("^dynamicAndStaticFiltersConjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, returnOperation));
    }

    @Test
    public void testExtractDynamicConjunct()
    {
        // the resulting blocks have the same name and parameters as the input block
        assertThat(extractDynamicConjunct(SINGLE_DYNAMIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new DynamicFilterExtractionResult(
                        SINGLE_DYNAMIC_FILTER,
                        truePredicate(SINGLE_DYNAMIC_FILTER.name(), SINGLE_DYNAMIC_FILTER.parameters(), new ProgramBuilder.ValueNameAllocator(100))));

        assertThat(extractDynamicConjunct(DYNAMIC_FILTERS_CONJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(extractionResultComparatorIgnoringDerivedAttributes())
                .isEqualTo(new DynamicFilterExtractionResult(
                        getRemappedDynamicFiltersConjunction(),
                        truePredicate(DYNAMIC_FILTERS_CONJUNCTION.name(), DYNAMIC_FILTERS_CONJUNCTION.parameters(), new ProgramBuilder.ValueNameAllocator(118))));

        assertThat(extractDynamicConjunct(DYNAMIC_AND_STATIC_FILTERS_CONJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(extractionResultComparatorIgnoringDerivedAttributes())
                .isEqualTo(new DynamicFilterExtractionResult(
                        getRemappedDynamicFilterConjunct(),
                        getRemappedStaticFilterConjunct()));

        assertThatThrownBy(() -> extractDynamicConjunct(NESTED_DYNAMIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .hasMessage("expected block returning boolean");

        assertThat(extractDynamicConjunct(DYNAMIC_FILTER_DISJUNCTION, new ProgramBuilder.ValueNameAllocator(100)))
                .usingComparator(extractionResultComparatorIgnoringDerivedAttributes())
                .isEqualTo(new DynamicFilterExtractionResult(
                        getMinimalDynamicFilterConjunct(),
                        getMaximalStaticFilterConjunct()));

        assertThat(extractDynamicConjunct(STATIC_FILTER, new ProgramBuilder.ValueNameAllocator(100)))
                .isEqualTo(new DynamicFilterExtractionResult(
                        truePredicate(STATIC_FILTER.name(), STATIC_FILTER.parameters(), new ProgramBuilder.ValueNameAllocator(100)),
                        STATIC_FILTER));
    }

    // the minimal dynamic conjunct from DYNAMIC_FILTER_DISJUNCTION, but with the return value remapped due to `PredicateUtils.extractLogicalTerms()`
    private static Block getMinimalDynamicFilterConjunct()
    {
        // dynamic filter
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(fieldReference.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(fieldReference.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));

        // static disjunct
        FieldReference fieldReference1 = new FieldReference("%6", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Logical disjunction = new Logical("%7", ImmutableList.of(dynamicFilterCall.result(), fieldReference1.result()), OR, ImmutableList.of(dynamicFilterCall.attributes(), fieldReference1.attributes()));

        Return returnOperation = new Return("%100", disjunction.result(), disjunction.attributes());

        return new Block(
                Optional.of("^dynamicFilterDisjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, fieldReference1, disjunction, returnOperation));
    }

    // the maximal static conjunct from DYNAMIC_FILTER_DISJUNCTION, but with the return value remapped due to `PredicateUtils.extractLogicalTerms()`
    private static Block getMaximalStaticFilterConjunct()
    {
        FieldReference fieldReference = new FieldReference("%8", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%101", fieldReference.result(), fieldReference.attributes());

        return new Block(
                Optional.of("^dynamicFilterDisjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, returnOperation));
    }

    private static Block getSingleDynamicFilter()
    {
        FieldReference field = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(field.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(field.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));
        Return returnOperation = new Return("%6", dynamicFilterCall.result(), dynamicFilterCall.attributes());

        return new Block(
                Optional.of("^singleDynamicFilter"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(field, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, returnOperation));
    }

    private static Block getDynamicFiltersConjunction()
    {
        // first dynamic filter
        FieldReference field1 = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator1 = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId1 = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed1 = new Constant("%3", BOOLEAN, false);
        Constant timeout1 = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall1 = new Call(
                "%5",
                ImmutableList.of(field1.result(), comparisonOperator1.result(), dynamicFilterId1.result(), nullAllowed1.result(), timeout1.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(field1.attributes(), comparisonOperator1.attributes(), dynamicFilterId1.attributes(), nullAllowed1.attributes(), timeout1.attributes()));

        // second dynamic filter
        FieldReference field2 = new FieldReference("%6", RELATION_ROW_PARAMETER, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator2 = new Constant("%7", VARCHAR, utf8Slice("<="));
        Constant dynamicFilterId2 = new Constant("%8", VARCHAR, utf8Slice("df_1"));
        Constant nullAllowed2 = new Constant("%8", BOOLEAN, true);
        Constant timeout2 = new Constant("%10", INTEGER, 5L);
        Call dynamicFilterCall2 = new Call(
                "%11",
                ImmutableList.of(field2.result(), comparisonOperator2.result(), dynamicFilterId2.result(), nullAllowed2.result(), timeout2.result()),
                DYNAMIC_FILTER_NULLABLE_FUNCTION,
                ImmutableList.of(field2.attributes(), comparisonOperator2.attributes(), dynamicFilterId2.attributes(), nullAllowed2.attributes(), timeout2.attributes()));

        Logical conjunction = new Logical("%12", ImmutableList.of(dynamicFilterCall1.result(), dynamicFilterCall2.result()), AND, ImmutableList.of(dynamicFilterCall1.attributes(), dynamicFilterCall2.attributes()));
        Return returnOperation = new Return("%13", conjunction.result(), conjunction.attributes());

        return new Block(
                Optional.of("^dynamicFiltersConjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(
                        field1,
                        comparisonOperator1,
                        dynamicFilterId1,
                        nullAllowed1,
                        timeout1,
                        dynamicFilterCall1,
                        field2,
                        comparisonOperator2,
                        dynamicFilterId2,
                        nullAllowed2,
                        timeout2,
                        dynamicFilterCall2,
                        conjunction,
                        returnOperation));
    }

    private static Block getDynamicAndStaticFiltersConjunction()
    {
        // dynamic filter
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(fieldReference.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(fieldReference.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));

        // static conjunct
        FieldReference fieldReference1 = new FieldReference("%6", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

        Logical conjunction = new Logical("%7", ImmutableList.of(dynamicFilterCall.result(), fieldReference1.result()), AND, ImmutableList.of(dynamicFilterCall.attributes(), fieldReference1.attributes()));
        Return returnOperation = new Return("%13", conjunction.result(), conjunction.attributes());

        return new Block(
                Optional.of("^dynamicAndStaticFiltersConjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, fieldReference1, conjunction, returnOperation));
    }

    private static Block getNestedDynamicFilter()
    {
        FieldReference field = new FieldReference("%1", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%2", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%3", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%4", BOOLEAN, false);
        Constant timeout = new Constant("%5", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%6",
                ImmutableList.of(field.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(field.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));
        Return dynamicFilterReturnOperation = new Return("%7", dynamicFilterCall.result(), dynamicFilterCall.attributes());

        Lambda lambda = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambdaBody"),
                        ImmutableList.of(RELATION_ROW_PARAMETER),
                        ImmutableList.of(field, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, dynamicFilterReturnOperation)));

        Return lambdaReturnOperation = new Return("%8", lambda.result(), lambda.attributes());

        return new Block(
                Optional.of("^nestedDynamicFilter"),
                ImmutableList.of(new Block.Parameter("%parameter", irType(anonymousRow(BIGINT)))),
                ImmutableList.of(lambda, lambdaReturnOperation));
    }

    private static Block getDynamicFilterDisjunction()
    {
        // dynamic filter
        FieldReference fieldReference = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant comparisonOperator = new Constant("%1", VARCHAR, utf8Slice(">"));
        Constant dynamicFilterId = new Constant("%2", VARCHAR, utf8Slice("df_0"));
        Constant nullAllowed = new Constant("%3", BOOLEAN, false);
        Constant timeout = new Constant("%4", INTEGER, null);
        Call dynamicFilterCall = new Call(
                "%5",
                ImmutableList.of(fieldReference.result(), comparisonOperator.result(), dynamicFilterId.result(), nullAllowed.result(), timeout.result()),
                DYNAMIC_FILTER_FUNCTION,
                ImmutableList.of(fieldReference.attributes(), comparisonOperator.attributes(), dynamicFilterId.attributes(), nullAllowed.attributes(), timeout.attributes()));

        // static disjunct
        FieldReference fieldReference1 = new FieldReference("%6", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Logical disjunction = new Logical("%7", ImmutableList.of(dynamicFilterCall.result(), fieldReference1.result()), OR, ImmutableList.of(dynamicFilterCall.attributes(), fieldReference1.attributes()));

        // another static conjunct
        FieldReference fieldReference2 = new FieldReference("%8", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Logical conjunction = new Logical("%9", ImmutableList.of(disjunction.result(), fieldReference2.result()), AND, ImmutableList.of(dynamicFilterCall.attributes(), fieldReference1.attributes()));

        Return returnOperation = new Return("%10", conjunction.result(), conjunction.attributes());

        return new Block(
                Optional.of("^dynamicFilterDisjunction"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(fieldReference, comparisonOperator, dynamicFilterId, nullAllowed, timeout, dynamicFilterCall, fieldReference1, disjunction, fieldReference2, conjunction, returnOperation));
    }

    private static Block getStaticFilter()
    {
        FieldReference field = new FieldReference("%0", RELATION_ROW_PARAMETER, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%1", field.result(), field.attributes());

        return new Block(
                Optional.of("^staticFilter"),
                ImmutableList.of(RELATION_ROW_PARAMETER),
                ImmutableList.of(field, returnOperation));
    }
}
