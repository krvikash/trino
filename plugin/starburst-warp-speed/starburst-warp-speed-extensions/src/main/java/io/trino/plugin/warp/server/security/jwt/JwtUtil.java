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
package io.trino.plugin.warp.server.security.jwt;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import io.jsonwebtoken.io.Serializer;
import io.jsonwebtoken.jackson.io.JacksonSerializer;

import java.util.Map;

// Mirrored from io.trino.server.security.jwt.JwtUtil in trino-main, with the parser
// builder dropped: warp only signs outbound bearers, so no JWT parsing happens here.
public final class JwtUtil
{
    private static final Serializer<Map<String, ?>> JWT_SERIALIZER = new JacksonSerializer<>();

    private JwtUtil() {}

    public static JwtBuilder newJwtBuilder()
    {
        return new DefaultJwtBuilder()
                .serializeToJsonWith(JWT_SERIALIZER);
    }
}
