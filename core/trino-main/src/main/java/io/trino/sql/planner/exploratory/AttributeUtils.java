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
package io.trino.sql.planner.exploratory;

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;

import java.util.List;
import java.util.Set;

public class AttributeUtils
{
    private AttributeUtils() {}

    /**
     * Derive attributes that hold for the whole group based on the current state of knowledge for the group and its operations.
     */
    public static Attributes deriveGroupAttributes(
            Attributes currentGroupAttributes,
            List<Attributes> operationsAttributes,
            DialectRegistry dialectRegistry)
    {
        Attributes.Builder derivedAttributes = Attributes.builder();
        dialectRegistry.dialects().forEach(dialect -> derivedAttributes.putAll(dialect.deriveGroupAttributes(currentGroupAttributes, operationsAttributes)));
        return derivedAttributes.buildOrThrow();
    }

    /**
     * Merge attributes of two groups.
     */
    public static Attributes mergeGroupAttributes(
            Attributes firstGroupAttributes,
            Attributes secondGroupAttributes,
            DialectRegistry dialectRegistry)
    {
        Attributes.Builder mergedAttributes = Attributes.builder();
        dialectRegistry.dialects().forEach(dialect -> mergedAttributes.putAll(dialect.mergeGroupAttributes(firstGroupAttributes, secondGroupAttributes)));
        return mergedAttributes.buildOrThrow();
    }

    /**
     * Compose attributes for an operation based on the current state of knowledge for the operation and its group.
     */
    public static Attributes composeOperationAttributes(
            Attributes allOperationAttributes,
            Attributes inherentOperationAttributes,
            Attributes groupAttributes,
            DialectRegistry dialectRegistry)
    {
        Attributes.Builder composedAttributes = Attributes.builder();
        composedAttributes.putAll(inherentOperationAttributes);
        dialectRegistry.dialects().forEach(dialect -> composedAttributes.putAll(dialect.composeOperationAttributes(allOperationAttributes, groupAttributes)));
        return composedAttributes.buildKeepingLast();
    }

    /**
     * Update attributes for an operation by applying freshly derived attributes onto the current state of knowledge for the operation.
     */
    public static Attributes updateOperationAttributes(
            Attributes allOperationAttributes,
            Attributes inherentOperationAttributes,
            Attributes derivedAttributes,
            DialectRegistry dialectRegistry)
    {
        Attributes.Builder updatedAttributes = Attributes.builder();
        updatedAttributes.putAll(inherentOperationAttributes);
        dialectRegistry.dialects().forEach(dialect -> updatedAttributes.putAll(dialect.updateOperationAttributes(allOperationAttributes, derivedAttributes)));
        return updatedAttributes.buildKeepingLast();
    }

    /**
     * Get inherent attributes of an operation.
     */
    public static Attributes getInherentAttributes(Operation operation, DialectRegistry dialectRegistry)
    {
        return getInherentAttributes(operation.dialect(), operation.id(), operation.attributes(), dialectRegistry);
    }

    public static Attributes getInherentAttributes(String dialect, OperationId operationId, Attributes attributes, DialectRegistry dialectRegistry)
    {
        return partitionAttributes(dialect, operationId, attributes, dialectRegistry).matching();
    }

    /**
     * Get derived attributes of an operation.
     */
    public static Attributes getDerivedAttributes(Operation operation, DialectRegistry dialectRegistry)
    {
        return getDerivedAttributes(operation.dialect(), operation.id(), operation.attributes(), dialectRegistry);
    }

    public static Attributes getDerivedAttributes(String dialect, OperationId operationId, Attributes attributes, DialectRegistry dialectRegistry)
    {
        return partitionAttributes(dialect, operationId, attributes, dialectRegistry).nonMatching();
    }

    private static Attributes.Partition partitionAttributes(
            String dialect,
            OperationId operationId,
            Attributes attributes,
            DialectRegistry dialectRegistry)
    {
        Dialect operationDialect = dialectRegistry.dialect(dialect);
        Set<AttributeKey> inherentAttributeKeys = operationDialect.getInherentOperationAttributeKeys(operationId);
        return attributes.partitionKeys(inherentAttributeKeys::contains);
    }
}
