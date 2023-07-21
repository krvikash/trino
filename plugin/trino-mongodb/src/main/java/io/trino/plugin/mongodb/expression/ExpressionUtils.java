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
package io.trino.plugin.mongodb.expression;

import org.bson.Document;

import java.util.Base64;

public class ExpressionUtils
{
    private ExpressionUtils() {}

    public static Document toDecimal(Object value)
    {
        return documentOf("$toDecimal", value);
    }

    public static Document toDate(Object value)
    {
        return documentOf("$toDate", value);
    }

    public static Document toObjectId(Object value)
    {
        return documentOf("$toObjectId", value);
    }

    public static Document toBinary(byte[] value)
    {
        return documentOf("$binary", documentOf("base64", Base64.getEncoder().encodeToString(value)).append("subType", "00"));
    }

    public static Document toString(Object value)
    {
        Document convertValue = documentOf("input", value)
                .append("to", "string")
                .append("onError", null);
        return documentOf("$convert", convertValue);
    }

    public static Document documentOf(String key, Object value)
    {
        return new Document(key, value);
    }
}
