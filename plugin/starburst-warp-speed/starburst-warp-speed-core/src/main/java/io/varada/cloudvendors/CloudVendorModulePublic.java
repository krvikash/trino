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
package io.varada.cloudvendors;

import com.google.inject.AbstractModule;
import io.varada.cloudvendors.config.CloudVendorConfig;

import java.lang.annotation.Annotation;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class CloudVendorModulePublic
        extends AbstractModule
        implements CloudVendorModule
{
    protected final String prefix;
    protected final Class<? extends Annotation> annotation;
    protected final Map<String, String> config;
    protected final Class<? extends CloudVendorConfig> configClazz;

    protected CloudVendorModulePublic(
            String prefix,
            Class<? extends Annotation> annotation,
            Map<String, String> config,
            Class<? extends CloudVendorConfig> configClazz)
    {
        this.prefix = prefix;
        this.annotation = requireNonNull(annotation);
        this.config = requireNonNull(config);
        this.configClazz = requireNonNull(configClazz);
    }
}
