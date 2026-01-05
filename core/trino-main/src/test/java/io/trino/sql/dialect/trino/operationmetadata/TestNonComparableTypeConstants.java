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

import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is to test the behavior of Constant operations involving values of non-comparable types.
 * Constant operation uses a ConstantValue object in its attribute to represent the constant value.
 * ConstantValue is similar to NullableValue in that it supports serialization and semantic comparison.
 * Additionally, ConstantValue objects implement safe equals() and hashCode() methods that do not
 * throw exceptions when the underlying type is non-comparable, while NullableValue's equals() and hashCode()
 * methods may throw exceptions in such cases.
 */
public class TestNonComparableTypeConstants
{
    @Test
    public void testNonComparableTypeConstants()
    {
        // comparing non-identical ConstantValues returns false
        assertThat(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).equals(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE)))
                .isFalse();

        // calling hashCode() on ConstantValue of incomparable type succeeds
        assertThat(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).hashCode())
                .isEqualTo(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).hashCode());

        Constant firstConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);
        Constant secondConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);

        // comparing constant operations does not throw exception
        assertThat(firstConstant.equals(secondConstant)).isFalse();

        // calling hashCode() on constant operation does not throw exception
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
    }
}
