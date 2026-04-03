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
package io.trino.operator.gpu;

import io.trino.spi.Page;

/**
 * Source {@link GpuOperation}.
 */
public interface GpuSourceOperation
        extends GpuOperation
{
    /**
     * Check if this source can accept more input.
     *
     * @return true if more input can be accepted
     */
    boolean needsInput();

    void addInput(Page page);

    void noMoreInput();

    interface Factory
    {
        GpuSourceOperation create();
    }
}
