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
package io.trino.plugin.kafka.schema.confluent;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.trino.decoder.json.JsonRowDecoder;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.spi.connector.ConnectorSession;
import org.everit.json.schema.Schema;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSessionProperties.getEmptyFieldStrategy;

public class JsonSchemaParser
        implements SchemaParser
{
    @Override
    public KafkaTopicFieldGroup parse(ConnectorSession session, String subject, ParsedSchema parsedSchema)
    {
        checkArgument(parsedSchema instanceof JsonSchema, "parsedSchema should be an instance of JsonSchema");
        Schema schema = ((JsonSchema) parsedSchema).rawSchema();
        JsonSchemaConverter schemaConverter = new JsonSchemaConverter(getEmptyFieldStrategy(session));
        List<KafkaTopicFieldDescription> fields = schemaConverter.convertJsonSchema(schema, subject);
        return new KafkaTopicFieldGroup(JsonRowDecoder.NAME, Optional.empty(), Optional.of(subject), fields);
    }
}
