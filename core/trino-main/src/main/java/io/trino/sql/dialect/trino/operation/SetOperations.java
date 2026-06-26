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

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static java.lang.String.format;

public class SetOperations
{
    private SetOperations() {}

    public static void validateSetOperation(List<Value> inputs, List<Block> inputFieldSelectors, List<Attributes> sourceAttributes, String operationName)
    {
        if (inputs.isEmpty()) {
            throw new TrinoException(IR_ERROR, format("inputs to the %s operation must not be empty", operationName));
        }
        if (inputs.size() != inputFieldSelectors.size()) {
            throw new TrinoException(IR_ERROR, format("inputs and input field selectors of %s operation do not match in size", operationName));
        }
        if (sourceAttributes.size() != inputs.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s for %s operation", sourceAttributes.size(), inputs.size(), operationName));
        }
        if (!inputs.stream()
                .allMatch(input -> IS_RELATION.test(trinoType(input.type())))) {
            throw new TrinoException(IR_ERROR, format("inputs to the %s operation must be of relation type", operationName));
        }
        for (int i = 0; i < inputFieldSelectors.size(); i++) {
            Block inputSelector = inputFieldSelectors.get(i);
            Value input = inputs.get(i);
            validateRowSelector(inputSelector, relationRowType(trinoType(input.type())), format("invalid input field selector for %s operation", operationName));
        }
        List<Type> outputFieldTypes = trinoType(inputFieldSelectors.getFirst().getReturnedType()).getTypeParameters();
        for (Block selector : inputFieldSelectors) {
            if (!outputFieldTypes.equals(trinoType(selector.getReturnedType()).getTypeParameters())) {
                throw new TrinoException(IR_ERROR, format("all input field selectors for %s operation must return the same type", operationName));
            }
        }
    }
}
