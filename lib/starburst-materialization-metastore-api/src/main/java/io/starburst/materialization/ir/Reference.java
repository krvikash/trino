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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A reference to a symbol bound elsewhere in the same persisted tree. See {@link Expression} for why the symbol
 * name must never be compared against a query plan.
 */
@JsonTypeName(Reference.NAME)
public record Reference(Symbol symbol)
        implements Expression
{
    static final String NAME = "Reference";
    static final int VERSION = 1;

    public Reference
    {
        requireNonNull(symbol, "symbol is null");
        // TODO(ENG-12666): once another node type can bind a symbol (not merely reference one), validate per
        //  document that every symbol name is bound exactly once and that every Reference resolves to a binding
    }

    @Override
    public Type type()
    {
        return symbol.type();
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

    @Override
    public Map<String, Integer> versionEntries()
    {
        return ImmutableMap.<String, Integer>builder()
                .putAll(Expression.super.versionEntries())
                .put(IrVersions.SYMBOL_LEAF, IrVersions.SYMBOL_LEAF_VERSION)
                .buildOrThrow();
    }
}
