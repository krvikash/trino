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
package io.starburst.stargate.icehouse.catalog;

import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergConfig.VariantMapping;
import io.trino.plugin.iceberg.IcebergTypeManager;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VariantType;
import io.trino.type.InternalTypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TypeConverter.toTrinoType casts its TypeManager argument to IcebergTypeManager on
 * the variant code path. A plain InternalTypeManager throws ClassCastException for any
 * table containing a variant column, so consumers that route schemas through Iceberg
 * type conversion must wrap their TypeManager in IcebergTypeManager.
 */
public class TestIcebergTypeManagerWrapping
{
    private static final IcebergTypeManager TYPE_MANAGER = new IcebergTypeManager(
            new InternalTypeManager(new TypeRegistry(new TypeOperators(), new FeaturesConfig())),
            VariantMapping.VARIANT);

    @Test
    public void testToTrinoTypeOnVariant()
    {
        Type trinoType = TypeConverter.toTrinoType(Types.VariantType.get(), TYPE_MANAGER);
        assertThat(trinoType).isEqualTo(VariantType.VARIANT);
    }

    @Test
    public void testGetTopLevelColumnsOnSchemaWithVariant()
    {
        Schema schema = new Schema(
                NestedField.optional(1, "id", Types.LongType.get()),
                NestedField.optional(2, "payload", Types.VariantType.get()));

        List<IcebergColumnHandle> columns = getTopLevelColumns(schema, TYPE_MANAGER);

        assertThat(columns).hasSize(2);
        assertThat(columns.get(1).getName()).isEqualTo("payload");
        assertThat(columns.get(1).getType()).isEqualTo(VariantType.VARIANT);
    }
}
