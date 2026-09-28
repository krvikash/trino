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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.CARDINALITY;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTrinoOperation
{
    @Test
    public void testReplaceAllArguments()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Value valueList = new Operation.Result("%value_list", irType(new ArrayType(BIGINT)));
        In in = new In("%in", input, valueList, TrinoOperation.emptySourceAttributes(2));

        Value newInput = new Operation.Result("%new_input", irType(BIGINT));
        Value newValueList = new Operation.Result("%new_value_list", irType(new ArrayType(BIGINT)));
        assertThat(in.withArguments(ImmutableList.of(newInput, newValueList)).arguments())
                .containsExactly(newInput, newValueList);

        // The index-based method remains available for replacing a single argument.
        assertThat(in.withArgument(newValueList, 1).arguments())
                .containsExactly(input, newValueList);
    }

    @Test
    public void testReplaceAllArgumentsValidation()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Value valueList = new Operation.Result("%value_list", irType(new ArrayType(BIGINT)));
        In in = new In("%in", input, valueList, TrinoOperation.emptySourceAttributes(2));

        assertThatThrownBy(() -> in.withArguments(ImmutableList.of(input)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("arguments list size mismatch");

        Value booleanValue = new Operation.Result("%boolean", irType(BOOLEAN));
        assertThatThrownBy(() -> in.withArguments(ImmutableList.of(input, booleanValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("argument type mismatch at index 1");
    }

    @Test
    public void testInValidation()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Value notAnArray = new Operation.Result("%not_an_array", irType(BIGINT));
        assertThatThrownBy(() -> new In("%in", input, notAnArray, TrinoOperation.emptySourceAttributes(2)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("value list must be of array type");

        Value wrongElementType = new Operation.Result("%wrong_element_type", irType(new ArrayType(BOOLEAN)));
        assertThatThrownBy(() -> new In("%in", input, wrongElementType, TrinoOperation.emptySourceAttributes(2)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("value list element type must match the input type");
    }

    @Test
    public void testDefaultReplaceAllArguments()
    {
        Value input = new Operation.Result("%input", irType(BIGINT));
        Return returnOperation = new Return("%return", input, Attributes.empty());
        Value newInput = new Operation.Result("%new_input", irType(BIGINT));

        assertThat(returnOperation.withArguments(ImmutableList.of(newInput)).arguments())
                .containsExactly(newInput);
    }

    @Test
    public void testReplaceRegions()
    {
        Block originalRow = rowBlock("original", BIGINT, 1L);
        Block replacementRow = rowBlock("replacement", BIGINT, 2L);
        AttributeKey enforcedAttribute = new AttributeKey("test", "enforced");
        Values values = new Values(
                "%values",
                anonymousRow(BIGINT),
                ImmutableList.of(originalRow),
                Attributes.builder()
                        .putUnchecked(enforcedAttribute, true)
                        .buildOrThrow());

        TrinoOperation copy = (TrinoOperation) values.withRegions(ImmutableList.of(singleBlockRegion(replacementRow)));

        assertThat(copy).isInstanceOf(Values.class);
        assertThat(copy.result()).isEqualTo(values.result());
        assertThat(copy.arguments()).isEqualTo(values.arguments());
        assertThat(copy.regions()).containsExactly(singleBlockRegion(replacementRow));
        assertThat(copy.operationAttributes()).isEqualTo(values.operationAttributes());
        assertThat(copy.attributes().get(enforcedAttribute)).isNull();
    }

    @Test
    public void testReplaceRegionsForValuesWithoutFields()
    {
        AttributeKey enforcedAttribute = new AttributeKey("test", "enforced");
        Values values = valuesWithoutFields(
                "%values",
                5,
                Attributes.builder()
                        .putUnchecked(enforcedAttribute, true)
                        .buildOrThrow());

        Operation copy = values.withRegions(ImmutableList.of());

        assertThat(copy).isNotSameAs(values);
        assertThat(copy.result()).isEqualTo(values.result());
        assertThat(copy.regions()).isEmpty();
        assertThat(CARDINALITY.getAttribute(copy.attributes())).isEqualTo(5L);
        assertThat(copy.attributes().get(enforcedAttribute)).isNull();
    }

    @Test
    public void testReplaceRegionsValidation()
    {
        Values values = new Values(
                "%values",
                anonymousRow(BIGINT),
                ImmutableList.of(rowBlock("original", BIGINT, 1L)));

        assertThatThrownBy(() -> values.withRegions(ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("regions list size mismatch");

        Region booleanRegion = singleBlockRegion(rowBlock("boolean", BOOLEAN, true));
        assertThatThrownBy(() -> values.withRegions(ImmutableList.of(booleanRegion)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("region type mismatch at index 0");
    }

    private static Block rowBlock(String name, Type type, Object value)
    {
        Constant constant = new Constant("%" + name + "_constant", type, value);
        Row row = new Row(
                "%" + name + "_row",
                ImmutableList.of(constant.result()),
                ImmutableList.of(constant.attributes()));
        Return returnOperation = new Return("%" + name + "_return", row.result(), row.attributes());
        return new Block(
                Optional.of("^" + name),
                ImmutableList.of(),
                ImmutableList.of(constant, row, returnOperation));
    }
}
