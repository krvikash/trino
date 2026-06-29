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
package io.starburst.materialization.metastore.client;

import io.airlift.http.client.Request;

import java.util.function.Consumer;

/**
 * Adds authentication details (token) to a request.
 * This will be either trino internal authentication that uses `X-Trino-Internal-Bearer` header,
 * or galaxy metastore authentication token using `Authorization` header.
 */
public interface RequestAuthenticator
        extends Consumer<Request.Builder> {}
