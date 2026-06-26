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
package io.trino.sql.dialect.trino.operationmetadata;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultComposeIrLevelAttributes;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalLongAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ValuesOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "values";

    private static final TrinoAttributeMetadata<Long> CARDINALITY_ATTRIBUTE_METADATA = internalLongAttributeMetadata(NAME, "cardinality");

    public static final TrinoAttributeSignature<Long> CARDINALITY = CARDINALITY_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Type> ROW_TYPE = new TrinoAttributeSignature<>(prefixedName(NAME, "row_type"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(CARDINALITY, ROW_TYPE);

    private final TrinoAttributeMetadata<Type> rowTypeTrinoAttributeMetadata;

    public ValuesOperationMetadata(Function<String, Type> typeDeserializer)
    {
        requireNonNull(typeDeserializer, "typeDeserializer is null");

        this.rowTypeTrinoAttributeMetadata = new TrinoAttributeMetadata<>(ROW_TYPE, typeDeserializer, type -> type.getTypeId().getId());
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(CARDINALITY_ATTRIBUTE_METADATA, rowTypeTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.isEmpty(), "Values operation does not have arguments");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        Values values;
        Type rowType = ROW_TYPE.getAttribute(operationAttributes);
        if (rowType.equals(EMPTY_ROW)) {
            values = valuesWithoutFields(resultName, toIntExact(CARDINALITY.getAttribute(operationAttributes)), derivedAttributes);
        }
        else {
            values = new Values(
                    resultName,
                    (RowType) rowType,
                    regions.stream()
                            .map(Region::getOnlyBlock)
                            .map(block -> block.withLabel("^row"))
                            .collect(toImmutableList()),
                    derivedAttributes);
        }

        return values;
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return ValuesOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        return defaultComposeIrLevelAttributes(childAttributes);
    }
}
