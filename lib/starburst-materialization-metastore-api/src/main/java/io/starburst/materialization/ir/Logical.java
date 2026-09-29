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

/**
 * A conjunction or disjunction. The engine's {@code canSplit} flag is an optimizer hint, not semantics, so it is
 * not persisted, and a matcher must ignore it on the query side.
 */
@JsonTypeName(Logical.NAME)
public record Logical(Operator operator, List<Expression> terms)
        implements Expression
{
    static final String NAME = "Logical";
    static final int VERSION = 1;

    public Logical
    {
        requireNonNull(operator, "operator is null");
        terms = ImmutableList.copyOf(requireNonNull(terms, "terms is null"));
        checkArgument(terms.size() >= 2, "Logical: expected at least 2 terms, got %s", terms.size());
        for (int i = 0; i < terms.size(); i++) {
            Type termType = terms.get(i).type();
            checkArgument(termType.equals(BOOLEAN), "Logical: term %s has type %s, expected boolean", i, describe(termType));
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
        return terms;
    }

    public enum Operator
    {
        AND,
        OR,
    }
}
