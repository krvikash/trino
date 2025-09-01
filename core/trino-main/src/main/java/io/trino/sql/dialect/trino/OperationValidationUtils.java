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
package io.trino.sql.dialect.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.newir.Block;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;

public class OperationValidationUtils
{
    private OperationValidationUtils()
    {}

    public static void validateRowSelector(Block block, Type inputType, String errorMessage)
    {
        if (block.parameters().size() != 1 ||
                !trinoType(block.parameters().getFirst().type()).equals(inputType) ||
                !(trinoType(block.getReturnedType()) instanceof RowType || trinoType(block.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }

    public static void validateRowSelectorReturningAtMostOneField(Block block, Type inputType, String errorMessage)
    {
        validateRowSelector(block, inputType, errorMessage);
        if (trinoType(block.getReturnedType()).getTypeParameters().size() > 1) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }

    public static void validateNonEmptyRowSelector(Block block, Type inputType, String errorMessage)
    {
        validateRowSelector(block, inputType, errorMessage);
        if (trinoType(block.getReturnedType()).equals(EMPTY_ROW)) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }

    public static void validatePredicate(Block block, Type inputType, String errorMessage)
    {
        if (block.parameters().size() != 1 ||
                !trinoType(block.parameters().getFirst().type()).equals(inputType) ||
                !trinoType(block.getReturnedType()).equals(BOOLEAN)) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }

    public static void validatePredicate(Block block, Type leftInputType, Type rightInputType, String errorMessage)
    {
        if (block.parameters().size() != 2 ||
                !trinoType(block.parameters().get(0).type()).equals(leftInputType) ||
                !trinoType(block.parameters().get(1).type()).equals(rightInputType) ||
                !trinoType(block.getReturnedType()).equals(BOOLEAN)) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }

    public static void validateRelationSelector(Block block, Type inputType, String errorMessage)
    {
        if (block.parameters().size() != 1 ||
                !trinoType(block.parameters().getFirst().type()).equals(inputType) ||
                !IS_RELATION.test(trinoType(block.getReturnedType()))) {
            throw new TrinoException(IR_ERROR, errorMessage);
        }
    }
}
