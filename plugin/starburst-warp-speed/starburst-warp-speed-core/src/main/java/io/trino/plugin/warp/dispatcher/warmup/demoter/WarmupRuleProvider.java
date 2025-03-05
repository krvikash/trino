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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import io.trino.plugin.warp.warmup.WarmupRuleService;
import io.trino.plugin.warp.warmup.model.WarmupRule;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

// this class is a temporary workaround until we separate connector & cache mgr classes
// better for the demoter
public class WarmupRuleProvider
{
    private final Optional<WarmupRuleService> optionalWarmupRuleService;

    public WarmupRuleProvider(Optional<WarmupRuleService> optionalWarmupRuleService)
    {
        this.optionalWarmupRuleService = requireNonNull(optionalWarmupRuleService);
    }

    public Collection<WarmupRule> getAll()
    {
        return optionalWarmupRuleService.map(WarmupRuleService::getAll).orElse(List.of());
    }
}
