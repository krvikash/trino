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
package io.trino.spi.connector;

import java.util.Optional;

public record GpuPageSourceSupport(boolean supported, Optional<String> reason)
{
    public static final GpuPageSourceSupport SUPPORTED = new GpuPageSourceSupport(true, Optional.empty());

    public static GpuPageSourceSupport unsupported(String reason)
    {
        return new GpuPageSourceSupport(false, Optional.of(reason));
    }

    public GpuPageSourceSupport
    {
        if (supported && reason.isPresent()) {
            throw new IllegalArgumentException("reason must be empty when supported");
        }
        if (!supported && reason.isEmpty()) {
            throw new IllegalArgumentException("reason must be present when unsupported");
        }
    }
}
