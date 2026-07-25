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
package io.trino.plugin.warp.util.json;

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

/**
 * Adapted from {@code io.trino.operator.scalar.JsonPath} to avoid dependency on trino-main.
 */
public class JsonPath
{
    private final String pattern;
    private final JsonExtract.JsonExtractor<Slice> scalarExtractor;

    public JsonPath(String pattern)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        scalarExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.ScalarValueJsonExtractor());
    }

    public String pattern()
    {
        return pattern;
    }

    public JsonExtract.JsonExtractor<Slice> getScalarExtractor()
    {
        return scalarExtractor;
    }

    @Override
    public String toString()
    {
        return pattern;
    }
}
