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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

/**
 * Scalar expression persisted in the materialization IR. The node set mirrors the supported subset of the
 * engine's {@code io.trino.sql.ir.Expression} node for node.
 * <p>
 * Symbol names inside an expression are engine-generated and meaningful only as bindings within one persisted
 * tree. A matcher must resolve a {@link Reference} back to whichever node in the same tree bound that symbol,
 * and compare identity through that binding (for example a {@link TableScan} assignment's
 * {@code ConnectorColumnId}) — never by comparing the symbol name itself against a query plan.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Cast.class, name = Cast.NAME),
        @JsonSubTypes.Type(value = In.class, name = In.NAME),
        @JsonSubTypes.Type(value = IsNull.class, name = IsNull.NAME),
        @JsonSubTypes.Type(value = Logical.class, name = Logical.NAME),
        @JsonSubTypes.Type(value = Reference.class, name = Reference.NAME),
})
public sealed interface Expression
        extends IrNode
        permits Cast,
                In,
                IsNull,
                Logical,
                Reference
{
    Type type();

    @Override
    List<Expression> children();

    /**
     * An {@code Expression} stamps its name under the {@value IrVersions#EXPRESSION_PREFIX} namespace. Nodes that
     * also commit the decoder to a shared leaf form, a function or a cast pair add those entries on top.
     */
    @Override
    default Map<String, Integer> versionEntries()
    {
        return ImmutableMap.of(IrVersions.expressionKey(name()), version());
    }
}
