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
package io.trino.server.testing;

import com.google.inject.Module;

/**
 * Marker SPI loaded via {@link java.util.ServiceLoader} by {@link TestingTrinoServer}
 * to install fork-specific Guice modules without a compile-time dependency on the
 * implementing module. Implementations must have a public no-arg constructor and be
 * registered in {@code META-INF/services/io.trino.server.testing.TestingServerExtensionModule}.
 */
public interface TestingServerExtensionModule
        extends Module {}
