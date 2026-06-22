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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.function.Function.identity;

public record JsonSchemaRoot(
        List<JsonMappingSchema> schemas)
{
    public JsonSchemaRoot
    {
        schemas = schemas == null ? ImmutableList.of() : ImmutableList.copyOf(schemas);
    }

    public List<String> listSchemas()
    {
        return schemas.stream()
                .map(s -> s.schema().toLowerCase(Locale.ENGLISH))
                .collect(toImmutableList());
    }

    public Map<String, JsonMappingSchema> mapSchemas()
    {
        return schemas.stream()
                .collect(toImmutableMap(
                        s -> s.schema().toLowerCase(Locale.ENGLISH),
                        identity(),
                        (a, b) -> { throw new TrinoException(GENERIC_INTERNAL_ERROR, "Duplicate schema name in JSON mapping (case-insensitive): '%s' and '%s'".formatted(a.schema(), b.schema())); }));
    }
}
