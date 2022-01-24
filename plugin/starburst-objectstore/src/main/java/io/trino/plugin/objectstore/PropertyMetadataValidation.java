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
package io.trino.plugin.objectstore;

import io.trino.spi.session.PropertyMetadata;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDefaultValue.VERIFY_DEFAULT_VALUE;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;

public final class PropertyMetadataValidation
{
    private PropertyMetadataValidation() {}

    public enum VerifyDefaultValue
    {
        VERIFY_DEFAULT_VALUE,
        IGNORE_DEFAULT_VALUE,
    }

    public enum VerifyDescription
    {
        VERIFY_DESCRIPTION,
        IGNORE_DESCRIPTION,
    }

    public static void verifyPropertyMetadata(PropertyMetadata<?> property, PropertyMetadata<?> existing)
    {
        verifyPropertyMetadata(property, existing, VERIFY_DEFAULT_VALUE, VERIFY_DESCRIPTION);
    }

    public static void verifyPropertyMetadata(PropertyMetadata<?> property, PropertyMetadata<?> existing, VerifyDefaultValue verifyDefaultValue, VerifyDescription verifyDescription)
    {
        verify(property.getName().equals(existing.getName()),
                "Mismatched name '%s' <> '%s' for property",
                property.getName(), existing.getName());

        // TODO verify hidden

        verify(property.getJavaType().equals(existing.getJavaType()),
                "Mismatched Java type '%s' <> '%s' for property: %s",
                property.getJavaType(), existing.getJavaType(), property.getName());

        verify(property.getSqlType().equals(existing.getSqlType()),
                "Mismatched SQL type '%s' <> '%s' for property: %s",
                property.getSqlType(), existing.getSqlType(), property.getName());

        switch (verifyDefaultValue) {
            case VERIFY_DEFAULT_VALUE -> verify(Objects.equals(property.getDefaultValue(), existing.getDefaultValue()),
                    "Mismatched default value '%s' <> '%s' for property: %s",
                    property.getDefaultValue(), existing.getDefaultValue(), property.getName());
            case IGNORE_DEFAULT_VALUE -> { /* ignored */ }
        }

        switch (verifyDescription) {
            case VERIFY_DESCRIPTION -> verify(property.getDescription().equals(existing.getDescription()),
                    "Mismatched description '%s' <> '%s' for property: %s",
                    property.getDescription(), existing.getDescription(), property.getName());
            case IGNORE_DESCRIPTION -> { /* ignored */ }
        }
    }

    public static void addProperty(Map<String, PropertyMetadata<?>> properties, PropertyMetadata<?> property)
    {
        checkArgument(!properties.containsKey(property.getName()), "Duplicate property: %s", property.getName());
        properties.put(property.getName(), property);
    }
}
