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
package io.trino.plugin.base;

import java.util.Collection;
import java.util.function.Supplier;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.stream.Gatherers.fold;

@FunctionalInterface
public interface Decorator<Decorated>
{
    static <Decorated> Decorated combine(Supplier<Decorated> initial, Collection<Decorator<Decorated>> decorators)
    {
        return decorators.stream()
                .gather(fold(initial, (decorated, decorator) -> decorator.decorate(decorated)))
                .collect(onlyElement());
    }

    Decorated decorate(Decorated decorated);
}
