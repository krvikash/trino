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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.materialization.ir.ConstantValue.NullValue;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A literal of a type {@link ConstantValues} supports. The constructor decodes the payload, so a payload this
 * build cannot decode can be neither built nor deserialized; a consumer re-derives the native value with
 * {@link ConstantValues#decode}.
 */
@JsonTypeName(Constant.NAME)
public record Constant(Type type, ConstantValue value)
        implements Expression
{
    static final String NAME = "Constant";
    static final int VERSION = 1;

    public Constant
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        ConstantValues.decode(type, value);
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public List<Expression> children()
    {
        return ImmutableList.of();
    }

    /**
     * A null stamps its kind only: no native representation can change what a stored null means, so stamping the
     * type's {@code ctype:} key would invalidate definitions no representation change can affect.
     */
    @Override
    public Map<String, Integer> versionEntries()
    {
        ImmutableMap.Builder<String, Integer> entries = ImmutableMap.<String, Integer>builder()
                .putAll(Expression.super.versionEntries())
                .put(IrVersions.TYPE_LEAF, IrVersions.TYPE_LEAF_VERSION)
                .put(IrVersions.constantKindKey(value.kind()), value.version());
        if (!(value instanceof NullValue)) {
            entries.put(IrVersions.constantTypeKey(type.getBaseName()), ConstantValues.encodingVersion(type));
        }
        return entries.buildOrThrow();
    }
}
