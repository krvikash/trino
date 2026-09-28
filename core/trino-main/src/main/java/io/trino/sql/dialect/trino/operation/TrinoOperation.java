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
package io.trino.sql.dialect.trino.operation;

import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public abstract class TrinoOperation
        extends Operation
// TODO sealed?
{
    public TrinoOperation(String dialect, String name)
    {
        super(dialect, name);
    }

    /**
     * Replace the operation's regions with newRegions.
     * The new regions must have the same count and respective types as the current regions.
     * The operation's result name and arguments are preserved.
     */
    public Operation withRegions(List<Region> newRegions)
    {
        validateRegions(newRegions);
        if (!regions().isEmpty()) {
            throw new UnsupportedOperationException(name() + " does not support region substitution");
        }
        return this;
    }

    void validateRegions(List<Region> newRegions)
    {
        requireNonNull(newRegions, "newRegions is null");

        List<Region> regions = regions();
        checkArgument(newRegions.size() == regions.size(), "regions list size mismatch");
        for (int i = 0; i < regions.size(); i++) {
            checkArgument(newRegions.get(i).getFunctionType().equals(regions.get(i).getFunctionType()), "region type mismatch at index %s", i);
        }
    }

    /**
     * Replace the operation's index-th argument with newArgument.
     * The newArgument must be of the same type as the current argument.
     */
    public Operation withArgument(Value newArgument, int index)
    {
        throw new UnsupportedOperationException(name() + " does not support argument substitution");
    }

    void validateArgument(Value newArgument, int index)
    {
        List<Value> arguments = arguments();
        checkArgument(index >= 0 && index < arguments.size(), "invalid argument index");
        checkArgument(newArgument.type().equals(arguments.get(index).type()), "argument type mismatch");
    }

    /**
     * Replace all operation arguments with newArguments.
     * The new arguments must have the same count and respective types as the current arguments.
     * Operations with variable arity should override this method to perform a single reconstruction.
     */
    public Operation withArguments(List<Value> newArguments)
    {
        validateArguments(newArguments);

        Operation operation = this;
        for (int i = 0; i < newArguments.size(); i++) {
            operation = ((TrinoOperation) operation).withArgument(newArguments.get(i), i);
        }
        return operation;
    }

    void validateArguments(List<Value> newArguments)
    {
        requireNonNull(newArguments, "newArguments is null");

        List<Value> arguments = arguments();
        checkArgument(newArguments.size() == arguments.size(), "arguments list size mismatch");
        for (int i = 0; i < arguments.size(); i++) {
            checkArgument(newArguments.get(i).type().equals(arguments.get(i).type()), "argument type mismatch at index %s", i);
        }
    }

    public Operation withResultName(String newName)
    {
        throw new UnsupportedOperationException(name() + " does not support result name substitution");
    }

    /**
     * Returns the attributes that are necessary for the operation because they determine its semantics.
     * Does not return the derived attributes.
     */
    public Attributes operationAttributes()
    {
        throw new UnsupportedOperationException("operationAttributes() not implemented for " + name());
    }

    Attributes filterAttributes(Set<TrinoAttributeSignature<?>> retained)
    {
        Set<AttributeKey> retainedKeys = retained.stream()
                .map(TrinoAttributeSignature::name)
                .map(name -> new AttributeKey(TRINO, name))
                .collect(toImmutableSet());
        return attributes().filterKeys(retainedKeys::contains);
    }

    public static List<Attributes> emptySourceAttributes(int sourceCount)
    {
        return nCopies(sourceCount, Attributes.empty());
    }

    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitOperation(this, context);
    }
}
