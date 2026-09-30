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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * The mapper every materialization IR codec must be built from. It rejects a missing or null primitive creator
 * parameter instead of silently reading it as zero or {@code false}, which a validating constructor cannot tell
 * apart from a real value. The rule covers every class in an IR document, including connector-owned
 * {@code ConnectorTableId} and {@code ConnectorColumnId} implementations, since none of them declare a setter or
 * field Jackson would otherwise auto-detect.
 */
public final class IrJsonMapper
{
    private IrJsonMapper() {}

    public static JsonMapper irJsonMapper(JsonMapper mapper)
    {
        return mapper.rebuild().enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES).build();
    }
}
