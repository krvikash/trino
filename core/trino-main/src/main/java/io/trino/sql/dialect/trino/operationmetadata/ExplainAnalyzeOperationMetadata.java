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
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;

public class ExplainAnalyzeOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "explain_analyze";

    private static final TrinoAttributeMetadata<Boolean> VERBOSE_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "verbose");

    public static final TrinoAttributeSignature<Boolean> VERBOSE = VERBOSE_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(VERBOSE_ATTRIBUTE_METADATA);
    }
}
