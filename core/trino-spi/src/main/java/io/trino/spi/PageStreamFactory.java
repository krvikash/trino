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
package io.trino.spi;

import java.io.InputStream;
import java.io.OutputStream;

public interface PageStreamFactory
{
    /**
     * Creates a reader that deserializes Pages from the provided stream.
     * <p>
     * Responsibilities:
     * <ul>
     *   <li>Caller: Creates and opens the stream, manages lifecycle of the underlying file</li>
     *   <li>Reader: Deserializes Pages, closes the stream when {@code close()} is called</li>
     * </ul>
     */
    PageStreamReader createReader(InputStream stream);

    /**
     * Creates a writer that serializes Pages to the provided stream.
     * <p>
     * Responsibilities:
     * <ul>
     *   <li>Caller: Creates and opens the stream, manages lifecycle of the underlying file</li>
     *   <li>Writer: Serializes Pages, closes the stream when {@code close()} is called</li>
     * </ul>
     */
    PageStreamWriter createWriter(OutputStream stream);
}
