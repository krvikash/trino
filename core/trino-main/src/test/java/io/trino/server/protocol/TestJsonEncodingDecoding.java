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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.JsonQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.server.protocol.AbstractTestEncodingDecoding.TypedColumn.typed;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonEncodingDecoding
        extends AbstractTestEncodingDecoding
{
    @Override
    protected QueryDataDecoder createDecoder(List<Column> columns)
    {
        return new JsonQueryDataDecoder.Factory().create(columns, DataAttributes.empty());
    }

    @Override
    protected QueryDataEncoder createEncoder(Session session, List<OutputColumn> columns)
    {
        return new JsonQueryDataEncoder.Factory().create(TEST_SESSION, columns);
    }

    @Test
    public void testInvalidJson()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BIGINT));

        assertInvalidJson(columns, "invalid", "Unrecognized token 'invalid'");
        assertInvalidJson(columns, "", "Expected start of an array, but got null");
        assertInvalidJson(columns, "[[]", "Unexpected token END_ARRAY");
        assertInvalidJson(columns, "[[", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5]", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5],]", "Unexpected character (']' (code 93))");
        assertInvalidJson(columns, "[[5][]", "Unexpected character ('[' (code 91))");

        assertThat(parseJson(columns, "[[5]]")).isEqualTo(List.of(List.of(5L)));
    }

    protected void assertInvalidJson(List<TypedColumn> columns, String json, String expectedError)
    {
        assertThatThrownBy(() -> parseJson(columns, json))
                .hasMessageContaining(expectedError);
    }

    protected List<List<Object>> parseJson(List<TypedColumn> columns, String json)
            throws IOException
    {
        QueryDataDecoder decoder = newDecoder(columns, true);
        return ImmutableList.copyOf(decoder.decode(new ByteArrayInputStream(json.getBytes(UTF_8)), null));
    }
}
