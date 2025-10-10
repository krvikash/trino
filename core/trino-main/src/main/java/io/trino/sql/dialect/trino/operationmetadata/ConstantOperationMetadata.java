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
import io.airlift.json.JsonCodec;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import org.assertj.core.util.VisibleForTesting;

import java.util.Set;
import java.util.function.Function;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;

public class ConstantOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "constant";

    public static final TrinoAttributeSignature<NullableValue> CONSTANT_VALUE = new TrinoAttributeSignature<>(prefixedName(NAME, "value"), false);

    private final TrinoAttributeMetadata<NullableValue> constantValueTrinoAttributeMetadata;

    public ConstantOperationMetadata(JsonCodec<NullableValue> nullableValueCodec)
    {
        this(nullableValueCodec::fromJson, nullableValueCodec::toJson);
    }

    @VisibleForTesting
    public ConstantOperationMetadata(Function<String, NullableValue> nullableValueParseMethod, Function<NullableValue, String> nullableValuePrintMethod)
    {
        requireNonNull(nullableValueParseMethod, "nullableValueParseMethod is null");
        requireNonNull(nullableValuePrintMethod, "nullableValuePrintMethod is null");

        this.constantValueTrinoAttributeMetadata = new TrinoAttributeMetadata<>(CONSTANT_VALUE, nullableValueParseMethod, nullableValuePrintMethod);
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(constantValueTrinoAttributeMetadata);
    }
}
