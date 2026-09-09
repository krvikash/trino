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
package io.trino.tracing;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.List;

public class TracingScopesConfig
{
    private List<String> enabledScopes = ImmutableList.of();

    public List<String> getEnabledScopes()
    {
        return enabledScopes;
    }

    @Config("tracing.enabled-scopes")
    @ConfigDescription("Comma-separated OpenTelemetry instrumentation scope name globs. When set, only " +
            "matching scopes produce spans and all other scopes are disabled. When unset (the default), " +
            "every scope produces spans as usual. Requires tracing.enabled=true — with tracing disabled " +
            "entirely, every scope is already a no-op and this property has no effect.")
    public TracingScopesConfig setEnabledScopes(List<String> enabledScopes)
    {
        this.enabledScopes = ImmutableList.copyOf(enabledScopes);
        return this;
    }
}
