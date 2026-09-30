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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.GatherMap;
import com.google.common.base.VerifyException;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;

record GatherMaps(@Own GatherMap probe, @Own GatherMap build)
        implements RuntimeCloseable
{
    public static @Move GatherMaps own(@Move GatherMap[] maps)
    {
        if (maps.length != 2) {
            throw closeAllSuppress(new VerifyException("Expected exactly 2 gather maps from join, got " + maps.length), maps);
        }
        return new GatherMaps(maps[0], maps[1]);
    }

    GatherMaps
    {
        requireNonNull(probe, "probe is null");
        requireNonNull(build, "build is null");
    }

    @Override
    public void close()
    {
        probe.close();
        build.close();
    }
}
