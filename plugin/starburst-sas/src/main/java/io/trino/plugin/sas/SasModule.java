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
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.plugin.sas.substitution.SasSubstitutionMetadata;

import java.net.URI;
import java.nio.file.Path;

import static com.google.inject.name.Names.named;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.plugin.sas.SasConfig.FILE_SCHEME;
import static io.trino.plugin.sas.SasConfig.S3_SCHEMES;
import static java.util.Locale.ENGLISH;

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
        binder.bind(SasSubstitutionMetadata.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SasConfig.class);
        SasConfig config = buildConfigObject(SasConfig.class);

        switch (config.getMappingType()) {
            case JSON -> binder.bind(SasClient.class).to(SasNasJsonClient.class).in(Scopes.SINGLETON);
            case FS -> binder.bind(SasClient.class).to(SasNasClient.class).in(Scopes.SINGLETON);
        }

        bindFileSystem(binder, config.getDataDirectory());

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(SasTable.class));
    }

    private void bindFileSystem(Binder binder, URI dataDirectory)
    {
        String scheme = dataDirectory.getScheme().toLowerCase(ENGLISH);
        Key<Location> rootLocationKey = Key.get(Location.class, named(ROOT_LOCATION_BINDING));
        if (scheme.equals(FILE_SCHEME)) {
            // Root the local file system at the data directory itself, so that every location
            // the connector handles is relative to it and cannot escape it
            binder.bind(TrinoFileSystemFactory.class).toInstance(new LocalFileSystemFactory(Path.of(dataDirectory)));
            binder.bind(rootLocationKey).toInstance(Location.of("local:///"));
            return;
        }
        if (S3_SCHEMES.contains(scheme)) {
            // Exposes the standard s3.* configuration properties (region, endpoint, credentials, IAM role, ...)
            install(new S3FileSystemModule());
            binder.bind(TrinoFileSystemFactory.class).to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class)).in(Scopes.SINGLETON);
            binder.bind(rootLocationKey).toInstance(s3RootLocation(scheme, dataDirectory));
            return;
        }
        // SasConfig validation rejects every other scheme before the module is set up
        throw new IllegalArgumentException("Unsupported sas.data-directory scheme: " + scheme);
    }

    private static Location s3RootLocation(String scheme, URI dataDirectory)
    {
        // URI preserves the scheme's case verbatim (e.g. "S3://..."), but S3Location only accepts
        // lowercase s3/s3a/s3n, so rebuild the location with the already-normalized scheme
        String location = scheme + dataDirectory.toString().substring(dataDirectory.getScheme().length());
        // Location requires a path separator after the bucket, even for a bucket root such as s3://bucket
        if (dataDirectory.getRawPath() == null || dataDirectory.getRawPath().isEmpty()) {
            location += "/";
        }
        return Location.of(location).removeOneTrailingSlash();
    }
}
