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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MappingsUtil
{
    private static final Logger LOG = Logger.get(MappingsUtil.class);
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
                else if (fieldName.equals("type") && (targetField.isTextual() || targetField.isArray()) && (sourceField.isTextual() || sourceField.isArray())) {
                    // In case of conflicting types for the same field, we merge types into an array of unique values.
                    // This approach violates the Elasticsearch mapping rules, as field type value must be a single string.
                    // However, we need to somehow pass information about conflicting fields downstream in order not to fail fast wildcard queries on many indexes.
                    // This way, downstream components can decide how to handle such conflicts, e.g. by failing the query if conflicting fields are involved or
                    // allow query if conflicting fields are not used in the query.
                    ImmutableSet.Builder<JsonNode> uniqueValues = ImmutableSortedSet.orderedBy(Comparator.comparing(JsonNode::asText));
                    uniqueValues.addAll(uniqueValuesFrom(targetField, fieldName));
                    uniqueValues.addAll(uniqueValuesFrom(sourceField, fieldName));

                    ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
                    arrayNode.addAll(uniqueValues.build());
                    targetNode.set(fieldName, arrayNode);
                }
                else {
                    throw new MergingMappingException(format("Mappings conflict detected. Conflicting values in mappings for field \"%s\" are: %s and %s", fieldName, targetField, sourceField));
                }
            }
            else {
                targetNode.set(fieldName, sourceField);
            }
        }
    }

    private static ImmutableSet<JsonNode> uniqueValuesFrom(JsonNode node, String fieldName)
    {
        ImmutableSet.Builder<JsonNode> uniqueNodes = ImmutableSet.builder();
        if (node.isArray()) {
            node.forEach(element -> {
                if (element.isTextual()) {
                    uniqueNodes.add(element);
                }
                else {
                    LOG.error("Unexpected non-textual element in array while merging mappings for field %s: %s", fieldName, element);
                }
            });
        }
        else {
            uniqueNodes.add(node);
        }
        return uniqueNodes.build();
    }
}
