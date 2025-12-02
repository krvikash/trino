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

import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;

public interface TrinoOperationMetadata
{
    String name();

    /**
     * Attributes that are necessary for the operation because they determine its semantics.
     * These attributes are operation-specific and should only be meaningful in the context of the operation.
     * In the future, we might not consider these attributes for propagation.
     */
    Set<TrinoAttributeMetadata<?>> operationAttributes();

    default Set<AttributeKey> operationAttributeKeys()
    {
        return operationAttributes().stream()
                .map(TrinoAttributeMetadata::trinoAttributeSignature)
                .map(TrinoAttributeSignature::name)
                .map(name -> new AttributeKey(TRINO, name))
                .collect(toImmutableSet());
    }

    /**
     * Function that derives attributes for the operation based on its current attributes and its children's attributes.
     */
    BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation();
}
