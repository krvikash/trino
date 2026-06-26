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
import io.trino.sql.dialect.trino.operation.SemiJoin;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalStringAttributeMetadata;

public class SemiJoinOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "semi_join";

    private static final TrinoAttributeMetadata<DistributionType> DISTRIBUTION_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "distribution_type", DistributionType.class);
    private static final TrinoAttributeMetadata<String> DYNAMIC_FILTER_ID_ATTRIBUTE_METADATA = internalStringAttributeMetadata(NAME, "dynamic_filter_id");

    public static final TrinoAttributeSignature<DistributionType> DISTRIBUTION_TYPE = DISTRIBUTION_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<String> DYNAMIC_FILTER_ID = DYNAMIC_FILTER_ID_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(DISTRIBUTION_TYPE, DYNAMIC_FILTER_ID);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(DISTRIBUTION_TYPE_ATTRIBUTE_METADATA, DYNAMIC_FILTER_ID_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.size() == 2, "SemiJoin operation must have exactly two arguments");
        checkArgument(regions.size() == 2, "SemiJoin operation must have exactly two regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new SemiJoin(
                resultName,
                arguments.get(0),
                arguments.get(1),
                regions.get(0).getOnlyBlock().withLabel("^sourceFieldSelector"),
                regions.get(1).getOnlyBlock().withLabel("^filteringSourceFieldSelector"),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(operationAttributes)),
                Optional.ofNullable(DYNAMIC_FILTER_ID.getAttribute(operationAttributes)),
                Attributes.empty(),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return SemiJoinOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 4, "SemiJoin operation must have exactly four child attributes maps: two for the inputs and one for each field selector");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED;

        public static DistributionType of(SemiJoinNode.DistributionType distributionType)
        {
            return switch (distributionType) {
                case PARTITIONED -> PARTITIONED;
                case REPLICATED -> REPLICATED;
            };
        }
    }
}
