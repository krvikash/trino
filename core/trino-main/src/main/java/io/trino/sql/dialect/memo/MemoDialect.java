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
package io.trino.sql.dialect.memo;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrAttributeDerivationUtils.passIrLevelAttributes;
import static java.lang.String.format;

/**
 * An auxiliary dialect used for the purpose of storing and processing Programs in Memo, not intended to be used outside of Memo.
 * <p>
 * Implicitly defines the Reuse operation and its reuse_id attribute. The Reuse operation is used to mark a reused group in Memo.
 * It helps to reason about the operation reuse and guides Program extraction from Memo. However, the Reuse operation itself
 * is not extracted as part of the final Program.
 */
public class MemoDialect
        extends Dialect
{
    public static final String MEMO = "memo";
    public static final MemoDialect MEMO_DIALECT = new MemoDialect();

    public static final String REUSE = "reuse";
    public static final String REUSE_ID = "reuse_id";

    private MemoDialect()
    {
        super(MEMO);
    }

    @Override
    public String formatAttribute(String name, Object attribute)
    {
        if (name.equals(REUSE_ID)) {
            if (!(attribute instanceof Integer)) {
                throw new TrinoException(IR_ERROR, format("the value of ir.%s attribute must be of type Integer. Actual: %s", name, attribute.getClass().getSimpleName()));
            }
            return attribute.toString();
        }
        throw new TrinoException(IR_ERROR, format("the memo dialect does not support attribute %s", name));
    }

    @Override
    public Object parseAttribute(String name, String attribute)
    {
        // Memo dialect supports the reuse_id attribute. This attribute is used only in Memo, and is not supposed to be encountered outside of Memo.
        throw new UnsupportedOperationException("parsing attributes is not supported for memo dialect");
    }

    @Override
    public String formatType(Type type)
    {
        throw new UnsupportedOperationException("the memo dialect does not support any types");
    }

    @Override
    public Type parseType(String type)
    {
        throw new UnsupportedOperationException("the memo dialect does not support any types");
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> getAttributeDerivationForOperation(OperationId id)
    {
        if (id.name().equals(REUSE)) {
            return (Attributes _, List<Attributes> childAttributes) -> {
                checkArgument(childAttributes.size() == 1, "Reuse operation must have exactly one child attribute set");
                return passIrLevelAttributes(getOnlyElement(childAttributes));
            };
        }
        throw new TrinoException(IR_ERROR, format("the memo dialect does not support operation %s", id.name()));
    }

    @Override
    public Set<AttributeKey> getInherentOperationAttributeKeys(OperationId id)
    {
        if (id.name().equals(REUSE)) {
            return ImmutableSet.of(new AttributeKey(MEMO, REUSE_ID));
        }
        throw new TrinoException(IR_ERROR, format("the memo dialect does not support operation %s", id.name()));
    }

    @Override
    public Operation createOperation(String name, String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        // Memo dialect supports the Reuse operation. The Reuse operation is used only in Memo, and it is not supposed to be created outside of Memo.
        throw new UnsupportedOperationException("the memo dialect does not support operation creation");
    }

    @Override
    public Attributes deriveGroupAttributes(Attributes currentGroupAttributes, List<Attributes> operationsAttributes)
    {
        // Memo dialect does not support any derived attributes.
        return Attributes.empty();
    }

    @Override
    public Attributes mergeGroupAttributes(Attributes firstGroupAttributes, Attributes secondGroupAttributes)
    {
        // Memo dialect does not support any derived attributes.
        return Attributes.empty();
    }

    @Override
    public Attributes composeOperationAttributes(Attributes operationAttributes, Attributes groupAttributes)
    {
        // Memo dialect does not support any derived attributes.
        return Attributes.empty();
    }

    @Override
    public Attributes updateOperationAttributes(Attributes operationAttributes, Attributes derivedAttributes)
    {
        // Memo dialect does not support any derived attributes.
        return Attributes.empty();
    }
}
