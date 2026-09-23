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
package io.trino.operator.gpu.window;

import static java.util.Objects.requireNonNull;

public record BoundTestedWindowFunction(String name, int[] argumentChannels)
{
    public BoundTestedWindowFunction
    {
        requireNonNull(name, "name is null");
        requireNonNull(argumentChannels, "argumentChannels is null");
    }

    public static BoundTestedWindowFunction of(String name, int... argumentChannels)
    {
        return new BoundTestedWindowFunction(name, argumentChannels);
    }

    public static BoundTestedWindowFunction of(TestedWindowFunction function, int... argumentChannels)
    {
        return of(function.name(), argumentChannels);
    }
}
