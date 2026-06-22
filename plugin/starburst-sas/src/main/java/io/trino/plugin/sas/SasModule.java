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
package io.trino.plugin.sas;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;

import java.nio.file.Path;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class SasModule
        extends AbstractConfigurationAwareModule
{
    static final String ROOT_LOCATION_BINDING = "data.rootLocation";

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SasConnector.class).in(Scopes.SINGLETON);
        binder.bind(SasMetadata.class).in(Scopes.SINGLETON);
        binder.bind(SasSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(SasRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SasConfig.class);

        switch (buildConfigObject(SasConfig.class).getMappingType()) {
            case JSON -> binder.bind(SasClient.class).to(SasNasJsonClient.class).in(Scopes.SINGLETON);
            case FS -> binder.bind(SasClient.class).to(SasNasClient.class).in(Scopes.SINGLETON);
        }

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(SasTable.class));
    }

    @Provides
    @Singleton
    @Named(ROOT_LOCATION_BINDING)
    static Location provideRootLocation()
    {
        // local:/// is relative to the LocalFileSystemFactory root (sas.data-directory);
        // for S3 this would be Location.of(config.getDataDirectory().toString()).removeOneTrailingSlash()
        return Location.of("local:///");
    }

    @Provides
    @Singleton
    static TrinoFileSystemFactory createFileSystemFactory(SasConfig config)
    {
        return new LocalFileSystemFactory(Path.of(config.getDataDirectory()));
    }
}
