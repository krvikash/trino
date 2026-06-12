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
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.exploratory.AttributeUtils.getDerivedAttributes;
import static java.util.Objects.requireNonNull;

/**
 * Enforcer is an Operation that can be inserted into the plan to enforce
 * a required property. It must satisfy the following conditions:
 * <ul>
 *     <li>has exactly one argument</li>
 *     <li>preserves the type of its argument</li>
 *     <li>preserves the derived attributes of its argument</li>
 * </ul>
 * TODO allow strengthening derived attributes
 */
public class Enforcer
{
    private final Operation operation;

    public Enforcer(Operation operation, Attributes derivedInputAttributes, DialectRegistry dialectRegistry)
    {
        requireNonNull(operation, "operation is null");
        requireNonNull(derivedInputAttributes, "derivedInputAttributes is null");
        requireNonNull(dialectRegistry, "dialectRegistry is null");

        checkArgument(operation.arguments().size() == 1, "enforcer operation must have one argument");
        checkArgument(operation.arguments().getFirst().type().equals(operation.result().type()), "enforcer operation must preserve type");
        Attributes derivedAttributes = getDerivedAttributes(operation, dialectRegistry);
        checkArgument(derivedAttributes.equals(derivedInputAttributes), "enforcer operation must preserve derived attributes");

        this.operation = operation;
    }

    public Operation operation()
    {
        return operation;
    }

    public Type type()
    {
        return operation.result().type();
    }
}
