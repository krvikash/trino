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
package io.trino.plugin.objectstore;

import io.opentelemetry.api.common.AttributeKey;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

final class TrinoAttributes
{
    private TrinoAttributes() {}

    static final AttributeKey<String> CATALOG = stringKey("trino.catalog");
    static final AttributeKey<String> SCHEMA = stringKey("trino.schema");
    static final AttributeKey<String> TABLE = stringKey("trino.table");
    static final AttributeKey<String> FUNCTION = stringKey("trino.function");
    static final AttributeKey<String> HANDLE = stringKey("trino.handle");
    static final AttributeKey<Boolean> CASCADE = booleanKey("trino.cascade");
}
