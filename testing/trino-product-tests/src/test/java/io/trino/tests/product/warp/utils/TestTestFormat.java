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
package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.tests.product.warp.utils.DemoterUtils.jsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTestFormat
{
    @Test
    public void testOverridingName()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("iceberg");
        assertThat(newTestFormat.name()).isEqualTo("overriding-name");
    }

    @Test
    public void testOverridingSkip()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("iceberg");
        assertThat(tests.getFirst().skip()).isEqualTo(false);
        assertThat(newTestFormat.skip()).isEqualTo(true);
    }

    @Test
    public void testOverridingSessionProperties()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("iceberg");
        assertThat(tests.getFirst().session_properties().get("enable_import_export")).isEqualTo(false);
        assertThat(newTestFormat.session_properties().get("enable_import_export")).isEqualTo(true);
    }

    @Test
    public void testOverridingWarmQuery()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("delta-lake");
        assertThat(tests.getFirst().warm_query()).isNotEqualTo(newTestFormat.warm_query());
    }

    @Test
    public void testOverridingQueries()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("delta-lake");
        Optional<TestFormat.QueryData> checkLimit1 = newTestFormat.queries_data().stream().filter(queryData -> queryData.query_id().equals("check_limit_1")).findFirst();
        assertThat(checkLimit1.orElseThrow().query()).isEqualTo("this is an overriding query");
        Optional<TestFormat.QueryData> additionalQuery = newTestFormat.queries_data().stream().filter(queryData -> queryData.query_id().equals("additional_query")).findFirst();
        assertThat(additionalQuery).isPresent();
    }

    @Test
    public void testNonExistOverriding()
            throws IOException
    {
        List<TestFormat> tests = jsonMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(new File("src/test/resources/warp-synthetic/simple.json"));
        TestFormat newTestFormat = TestFormat.builder(tests.getFirst()).build("not-exist");
        assertThat(tests.getFirst()).isEqualTo(newTestFormat);
    }
}
