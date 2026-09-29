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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.materialization.ir.IrMessages.describe;
import static java.util.Objects.requireNonNull;

/**
 * Access to the {@code field}-th field, zero-based, of a row-typed expression.
 */
@JsonTypeName(FieldReference.NAME)
public record FieldReference(Expression base, int field)
        implements Expression
{
    static final String NAME = "FieldReference";
    static final int VERSION = 1;

    public FieldReference
    {
        requireNonNull(base, "base is null");
        checkArgument(base.type() instanceof RowType, "FieldReference: base has type %s, expected a row type", describe(base.type()));
        int fieldCount = ((RowType) base.type()).getFields().size();
        checkArgument(field >= 0 && field < fieldCount, "FieldReference: field %s is out of bounds for a row with %s fields", field, fieldCount);
    }

    @Override
    public Type type()
    {
        return ((RowType) base.type()).getFields().get(field).getType();
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
        return ImmutableList.of(base);
    }
}
