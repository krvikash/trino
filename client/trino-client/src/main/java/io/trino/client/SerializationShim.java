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
package io.trino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.trino.client.spooling.Segment;

import java.util.List;

import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.client.TrinoJsonCodec.listJsonCodec;

public class SerializationShim
{
    private static final TrinoJsonCodec<Segment> SEGMENT_CODEC = jsonCodec(Segment.class);
    private static final TrinoJsonCodec<List<Column>> COLUMNS_CODEC = listJsonCodec(Column.class);

    private SerializationShim() {}

    public static String fromSegment(Segment segment)
    {
        return SEGMENT_CODEC.toJson(segment);
    }

    public static Segment toSegment(String json)
    {
        try {
            return SEGMENT_CODEC.fromJson(json);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String fromColumns(List<Column> columns)
    {
        return COLUMNS_CODEC.toJson(columns);
    }

    public static List<Column> toColumns(String json)
    {
        try {
            return COLUMNS_CODEC.fromJson(json);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
