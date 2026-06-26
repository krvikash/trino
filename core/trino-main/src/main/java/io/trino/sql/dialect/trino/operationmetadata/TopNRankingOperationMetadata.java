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
import io.trino.sql.dialect.ir.IrAttributeUtils;
import io.trino.sql.dialect.trino.operation.TopNRanking;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.plan.TopNRankingNode;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonIdempotent;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;

public class TopNRankingOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "top_n_ranking";

    private static final TrinoAttributeMetadata<RankingType> RANKING_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "ranking_type", RankingType.class);
    private static final TrinoAttributeMetadata<Integer> MAX_RANKING_PER_PARTITION_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "max_ranking_per_partition");
    private static final TrinoAttributeMetadata<Boolean> PARTIAL_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "partial");
    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");

    public static final TrinoAttributeSignature<RankingType> RANKING_TYPE = RANKING_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Integer> MAX_RANKING_PER_PARTITION = MAX_RANKING_PER_PARTITION_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> PARTIAL = PARTIAL_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(RANKING_TYPE, MAX_RANKING_PER_PARTITION, PARTIAL, SORT_ORDERS);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                RANKING_TYPE_ATTRIBUTE_METADATA,
                MAX_RANKING_PER_PARTITION_ATTRIBUTE_METADATA,
                PARTIAL_ATTRIBUTE_METADATA,
                SORT_ORDERS_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        checkArgument(arguments.size() == 1, "TopNRanking operation must have exactly one argument: the input relation");
        checkArgument(regions.size() == 2, "TopNRanking operation must have exactly two regions");

        Attributes.Partition partitionedAttributes = attributes.partitionKeys(inherentOperationAttributeKeys()::contains);
        Attributes operationAttributes = partitionedAttributes.matching();
        Attributes derivedAttributes = partitionedAttributes.nonMatching();

        return new TopNRanking(
                resultName,
                getOnlyElement(arguments),
                regions.get(0).getOnlyBlock().withLabel("^partitioningSelector"),
                regions.get(1).getOnlyBlock().withLabel("^orderingSelector"),
                RANKING_TYPE.getAttribute(operationAttributes),
                MAX_RANKING_PER_PARTITION.getAttribute(operationAttributes),
                PARTIAL.getAttribute(operationAttributes),
                SORT_ORDERS.getAttribute(operationAttributes),
                Attributes.empty(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> attributeDerivation()
    {
        return TopNRankingOperationMetadata::deriveAttributes;
    }

    public static Attributes deriveAttributes(Attributes currentAttributes, List<Attributes> childAttributes)
    {
        checkArgument(childAttributes.size() == 3, "TopNRanking operation must have exactly three child attributes maps: one for the input, and one for each selector");

        Attributes.Builder derivedAttributes = Attributes.builder();

        // TopNRanking operation is non-idempotent in case there are ties
        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            nonIdempotent(derivedAttributes);
        }

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }

    public enum RankingType
    {
        ROW_NUMBER,
        RANK,
        DENSE_RANK;

        public static RankingType of(TopNRankingNode.RankingType rankingType)
        {
            return switch (rankingType) {
                case ROW_NUMBER -> ROW_NUMBER;
                case RANK -> RANK;
                case DENSE_RANK -> DENSE_RANK;
            };
        }
    }
}
