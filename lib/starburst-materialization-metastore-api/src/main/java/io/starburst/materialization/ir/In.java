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
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.materialization.ir.IrMessages.describe;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

@JsonTypeName(In.NAME)
public record In(Expression value, List<Expression> valueList)
        implements Expression
{
    static final String NAME = "In";
    static final int VERSION = 1;

    public In
    {
        requireNonNull(value, "value is null");
        valueList = ImmutableList.copyOf(requireNonNull(valueList, "valueList is null"));
        checkArgument(!valueList.isEmpty(), "In: value list is empty");
        for (int i = 0; i < valueList.size(); i++) {
            Type itemType = valueList.get(i).type();
            checkArgument(itemType.equals(value.type()), "In: item %s has type %s, expected %s", i, describe(itemType), describe(value.type()));
        }
    }

    @Override
    public Type type()
    {
        return BOOLEAN;
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
        return ImmutableList.<Expression>builder()
                .add(value)
                .addAll(valueList)
                .build();
    }
}
