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
package io.trino.sql.dialect.trino.operationmetadata;

import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.CastKind;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownSafe;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.SAFE;
import static io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.CAST_KIND;
import static io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.TO_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class TestCastOperationMetadata
{
    private static final Attributes SAFE_ATTRIBUTES = Attributes.builder()
            .putUnchecked(new AttributeKey(IR, SAFE), true)
            .buildOrThrow();

    @Test
    void testDeriveSafety()
    {
        assertThat(isSafe(BIGINT, CastKind.REINTERPRET, SAFE_ATTRIBUTES)).isTrue();
        assertThat(isSafe(VARCHAR, CastKind.CONVERT, SAFE_ATTRIBUTES)).isTrue();

        assertThat(isSafe(createVarcharType(10), CastKind.CONVERT, SAFE_ATTRIBUTES)).isFalse();
        assertThat(isSafe(BIGINT, CastKind.CONVERT, SAFE_ATTRIBUTES)).isFalse();
        assertThat(isSafe(BIGINT, CastKind.REINTERPRET, Attributes.empty())).isFalse();
    }

    @Test
    void testDerivationIsIdempotent()
    {
        Attributes operationAttributes = castAttributes(VARCHAR, CastKind.CONVERT);
        Attributes derivedAttributes = CastOperationMetadata.deriveAttributes(operationAttributes, List.of(SAFE_ATTRIBUTES));
        Attributes attributesWithDerivedKnowledge = Attributes.builder()
                .putAll(operationAttributes)
                .putAll(derivedAttributes)
                .buildOrThrow();

        assertThat(CastOperationMetadata.deriveAttributes(attributesWithDerivedKnowledge, List.of(SAFE_ATTRIBUTES)))
                .isEqualTo(derivedAttributes);
    }

    private static boolean isSafe(Type targetType, CastKind castKind, Attributes sourceAttributes)
    {
        return isKnownSafe(CastOperationMetadata.deriveAttributes(castAttributes(targetType, castKind), List.of(sourceAttributes)));
    }

    private static Attributes castAttributes(Type targetType, CastKind castKind)
    {
        return Attributes.builder()
                .putAll(TO_TYPE.asAttributes(targetType))
                .putAll(CAST_KIND.asAttributes(castKind))
                .buildOrThrow();
    }
}
