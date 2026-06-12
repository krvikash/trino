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
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Type;

import java.util.List;
import java.util.Set;

public class MemoOperationBuilder
{
    private String dialect;
    private Operation.OperationId operationId;
    private Type resultType;
    private List<Type> groupParameterTypes;
    private List<MemoOperation.Child> children;
    private Attributes attributes;
    private Set<AttributeKey> inherentOperationAttributeKeys;

    public static MemoOperationBuilder from(MemoOperation memoOperation)
    {
        return new MemoOperationBuilder(memoOperation);
    }

    private MemoOperationBuilder(MemoOperation memoOperation)
    {
        this.dialect = memoOperation.dialect();
        this.operationId = memoOperation.operationId();
        this.resultType = memoOperation.resultType();
        this.groupParameterTypes = memoOperation.groupParameterTypes();
        this.children = memoOperation.children();
        this.attributes = memoOperation.attributes();
        this.inherentOperationAttributeKeys = memoOperation.inherentOperationAttributeKeys();
    }

    public MemoOperationBuilder withDialect(String dialect)
    {
        this.dialect = dialect;
        return this;
    }

    public MemoOperationBuilder withOperationId(Operation.OperationId operationId)
    {
        this.operationId = operationId;
        return this;
    }

    public MemoOperationBuilder withResultType(Type resultType)
    {
        this.resultType = resultType;
        return this;
    }

    public MemoOperationBuilder withGroupParameterTypes(List<Type> groupParameterTypes)
    {
        this.groupParameterTypes = groupParameterTypes;
        return this;
    }

    public MemoOperationBuilder withChildren(List<MemoOperation.Child> children)
    {
        this.children = children;
        return this;
    }

    public MemoOperationBuilder withAttributes(Attributes attributes)
    {
        this.attributes = attributes;
        return this;
    }

    public MemoOperationBuilder withInherentOperationAttributeKeys(Set<AttributeKey> inherentOperationAttributeKeys)
    {
        this.inherentOperationAttributeKeys = inherentOperationAttributeKeys;
        return this;
    }

    public MemoOperation build()
    {
        return new MemoOperation(
                dialect,
                operationId,
                resultType,
                groupParameterTypes,
                children,
                attributes,
                inherentOperationAttributeKeys);
    }
}
