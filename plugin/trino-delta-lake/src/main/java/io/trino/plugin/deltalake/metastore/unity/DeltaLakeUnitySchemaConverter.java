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
package io.trino.plugin.deltalake.metastore.unity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import io.unitycatalog.client.JSON;
import io.unitycatalog.client.delta.model.DeltaArrayType;
import io.unitycatalog.client.delta.model.DeltaDataType;
import io.unitycatalog.client.delta.model.DeltaMapType;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.delta.serde.DeltaDataTypeModule;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;

final class DeltaLakeUnitySchemaConverter
{
    // The Delta log schema uses camelCase for array and map fields, while the Delta API uses kebab-case.
    // Mixins allow us to associate these properties to convert between the two casing styles.
    private static final ObjectMapper DELTA_LOG_SCHEMA_MAPPER = JSON.getDefault().getMapper().copy()
            .registerModule(new DeltaDataTypeModule())
            .addMixIn(DeltaArrayType.class, DeltaLogArrayTypeMixin.class)
            .addMixIn(DeltaMapType.class, DeltaLogMapTypeMixin.class);

    private DeltaLakeUnitySchemaConverter() {}

    static DeltaStructType toDeltaStructType(String schemaString)
    {
        try {
            return DELTA_LOG_SCHEMA_MAPPER.readValue(schemaString, DeltaStructType.class);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Invalid Delta schema string", e);
        }
    }

    private abstract static class DeltaLogArrayTypeMixin
    {
        @JsonProperty("elementType")
        abstract DeltaDataType getElementType();

        @JsonProperty("elementType")
        abstract void setElementType(DeltaDataType elementType);

        @JsonProperty("containsNull")
        abstract Boolean getContainsNull();

        @JsonProperty("containsNull")
        abstract void setContainsNull(Boolean containsNull);
    }

    private abstract static class DeltaLogMapTypeMixin
    {
        @JsonProperty("keyType")
        abstract DeltaDataType getKeyType();

        @JsonProperty("keyType")
        abstract void setKeyType(DeltaDataType keyType);

        @JsonProperty("valueType")
        abstract DeltaDataType getValueType();

        @JsonProperty("valueType")
        abstract void setValueType(DeltaDataType valueType);

        @JsonProperty("valueContainsNull")
        abstract Boolean getValueContainsNull();

        @JsonProperty("valueContainsNull")
        abstract void setValueContainsNull(Boolean valueContainsNull);
    }
}
