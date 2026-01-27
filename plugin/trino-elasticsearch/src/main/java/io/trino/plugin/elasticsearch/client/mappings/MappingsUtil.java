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
package io.trino.plugin.elasticsearch.client.mappings;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.json.ObjectMapperProvider;

import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MappingsUtil
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private MappingsUtil() {}

    public static JsonNode union(List<JsonNode> jsonNodes)
            throws MergingMappingException
    {
        requireNonNull(jsonNodes, "argument jsonNodes is null");
        ObjectNode resultNode = OBJECT_MAPPER.createObjectNode();
        if (jsonNodes.isEmpty()) {
            return resultNode;
        }

        for (JsonNode jsonNode : jsonNodes) {
            if (!jsonNode.isObject()) {
                throw new MergingMappingException("Mappings must be JSON objects");
            }
            copy((ObjectNode) jsonNode, resultNode);
        }
        return resultNode;
    }

    private static void copy(ObjectNode sourceNode, ObjectNode targetNode)
            throws MergingMappingException
    {
        Iterator<String> fieldNames = sourceNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode sourceField = sourceNode.get(fieldName);
            JsonNode targetField = targetNode.get(fieldName);

            if (targetField != null) {
                if (targetField.isObject() && sourceField.isObject()) {
                    copy((ObjectNode) sourceField, (ObjectNode) targetField);
                }
                else if (targetField.equals(sourceField)) {
                    targetNode.set(fieldName, sourceField);
                }
                else {
                    throw new MergingMappingException(format("Mappings conflict detected. Conflicting values in mappings for field %s are: %s and %s", fieldName, targetField, sourceField));
                }
            }
            else {
                targetNode.set(fieldName, sourceField);
            }
        }
    }
}
