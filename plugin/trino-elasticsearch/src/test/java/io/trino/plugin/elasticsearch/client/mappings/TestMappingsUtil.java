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
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.plugin.elasticsearch.client.mappings.MappingsUtil.union;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestMappingsUtil
{
    @Test
    void testCreateUnionOfNull()
    {
        assertThatThrownBy(() -> union(null))
                .hasMessageContaining("argument jsonNodes is null");
    }

    @Test
    void testCreateUnionOfEmptyMappingsList()
            throws MergingMappingException
    {
        JsonNode actual = union(ImmutableList.of());
        assertThat(actual).isEqualTo(parseJson("{}", JsonNode.class));
    }

    @Test
    void testCreateUnionOfMappingsListWithEmptyNode()
            throws MergingMappingException
    {
        JsonNode actual = union(ImmutableList.of(parseJson("{}", JsonNode.class)));
        assertThat(actual).isEqualTo(parseJson("{}", JsonNode.class));
    }

    @Test
    void testShouldThrowExceptionOnNonObjectNode()
    {
        assertThatThrownBy(() -> union(ImmutableList.of(parseJson("42", JsonNode.class))))
                .hasMessage("Mappings must be JSON objects");

        assertThatThrownBy(() -> union(ImmutableList.of(parseJson("true", JsonNode.class))))
                .hasMessage("Mappings must be JSON objects");
    }

    @Test
    void testCreateUnionSameMappings()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionLeftHasMoreFields()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionRightHasMoreFields()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;
        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionBothHaveMoreFields()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;
        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionWhenMissingType()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "name":   { }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;
        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionWhenMissingTypeForUniqueField()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "email":  { "type": "keyword" }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "email":  { "type": "keyword" },
                      "name":   { }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "email":  { "type": "keyword"  },
                      "name":   { }
                    }
                  }
                }
                """;

        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testCreateUnionBothHaveMoreFieldsInNested()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "id": { "type": "keyword" },
                      "address": {
                        "type": "object",
                        "properties": {
                          "city": { "type": "keyword" },
                          "zip": { "type": "integer" }
                        }
                      }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "id": { "type": "keyword" },
                      "address": {
                        "type": "object",
                        "properties": {
                          "city": { "type": "keyword" },
                          "street": { "type": "keyword" }
                        }
                      }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "id": { "type": "keyword" },
                      "address": {
                        "type": "object",
                        "properties": {
                          "city": { "type": "keyword" },
                          "zip": { "type": "integer" },
                          "street": { "type": "keyword" }
                        }
                      }
                    }
                  }
                }
                """;
        assertUnion(mappings1, mappings2, expectedMappings);
    }

    @Test
    void testShouldCreateConflictTypeWithArrayOfTypeWhenInconsistentMappingForField()
            throws MergingMappingException
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "long" },
                      "email":  { "type": "string"  },
                      "name":   { "type": "keyword"  }
                    }
                  }
                }
                """;

        String expectedMappings = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": ["integer","long"] },
                      "email":  { "type": ["keyword","string"]  },
                      "name":   { "type": ["keyword", "text"]  }
                    }
                  }
                }
                """;

        assertUnion(ImmutableList.of(mappings1, mappings2), expectedMappings);
        assertUnion(ImmutableList.of(mappings2, mappings1), expectedMappings);
        assertUnion(ImmutableList.of(mappings1, mappings2, mappings1), expectedMappings);
        assertUnion(ImmutableList.of(mappings1, mappings2, mappings1, mappings1, mappings1), expectedMappings);
        assertUnion(ImmutableList.of(mappings1, mappings2, mappings1, mappings2, mappings2), expectedMappings);
        assertUnion(ImmutableList.of(mappings2, mappings2, mappings2, mappings1), expectedMappings);
    }

    @Test
    void testShouldNotCreateArrayOfTypesForPropertyNameOtherThanType()
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type1": "integer" },
                      "email":  { "type2": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type1": "string" },
                      "email":  { "type2": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        assertThatThrownBy(() -> union(ImmutableList.of(parseJson(mappings1, JsonNode.class), parseJson(mappings2, JsonNode.class))))
                .hasMessage("Mappings conflict detected. Conflicting values in mappings for field \"type1\" are: \"integer\" and \"string\"");
    }

    @Test
    void testShouldComplainOnNotCompatibleFields()
    {
        String mappings1 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name": "text"
                    }
                  }
                }
                """;

        String mappings2 = """
                {
                  "mappings": {
                    "properties": {
                      "age":    { "type": "integer" },
                      "email":  { "type": "keyword"  },
                      "name":   { "type": "text"  }
                    }
                  }
                }
                """;

        assertThatThrownBy(() -> union(ImmutableList.of(parseJson(mappings1, JsonNode.class), parseJson(mappings2, JsonNode.class))))
                .hasMessage("Mappings conflict detected. Conflicting values in mappings for field \"name\" are: \"text\" and {\"type\":\"text\"}");
    }

    private static void assertUnion(String json1, String json2, String expected)
            throws MergingMappingException
    {
        assertUnion(ImmutableList.of(json1, json2), expected);
    }

    private static void assertUnion(ImmutableList<String> jsons, String expected)
            throws MergingMappingException
    {
        JsonNode actual = union(jsons.stream().map(json -> parseJson(json, JsonNode.class)).collect(toImmutableList()));
        assertThat(actual).isEqualTo(parseJson(expected, JsonNode.class));
    }
}
