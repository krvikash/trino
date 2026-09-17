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
package io.trino.filesystem.azure;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.util.Optional;

import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.HIERARCHICAL;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Runs the file system contract against a Microsoft Fabric workspace. The workspace stands in for
 * the container, since it cannot be created through the storage API, so each run owns a directory
 * under the Fabric item instead.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestAzureFileSystemOneLake
        extends AbstractTestAzureFileSystem
{
    private String workspace;
    private String pathPrefix;

    @BeforeAll
    void setup()
            throws IOException
    {
        String region = requireEnv("ONELAKE_REGION");
        workspace = requireEnv("ONELAKE_WORKSPACE");
        String item = requireEnv("ONELAKE_ITEM");
        pathPrefix = "%s/Files/test-%s".formatted(item, randomUUID());

        initializeWithOAuth(
                "%s-onelake".formatted(region),
                requireEnv("AZURE_ABFS_OAUTH_TENANT_ID"),
                requireEnv("AZURE_ABFS_OAUTH_CLIENT_ID"),
                requireEnv("AZURE_ABFS_OAUTH_CLIENT_SECRET"),
                HIERARCHICAL);
    }

    @Override
    protected String endpoint()
    {
        return "fabric.microsoft.com";
    }

    @Override
    protected Optional<String> existingContainer()
    {
        return Optional.of(workspace);
    }

    @Override
    protected String pathPrefix()
    {
        return pathPrefix;
    }

    @Override
    protected boolean supportsAccountKindProbe()
    {
        // OneLake is hierarchical, but Fabric refuses the probe against an item root
        return false;
    }

    @Test
    @Override
    public void testPreSignedUris()
    {
        abort("OneLake rejects the user delegation key request that pre-signed URIs need");
    }

    @Test
    @Override
    public void testPaths()
            throws IOException
    {
        // OneLake's root directory is inside a Fabric item, so `..` resolves to a valid path instead of escaping the root
        try (TempBlob absolute = new TempBlob(createLocation("b"))) {
            try (TempBlob alias = new TempBlob(createLocation("a/../b"))) {
                absolute.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + absolute.location().toString());
                assertThat(alias.exists()).isTrue();
                assertThat(absolute.exists()).isTrue();

                assertThat(alias.read()).isEqualTo(TEST_BLOB_CONTENT_PREFIX + absolute.location());

                assertThat(listPath("")).containsExactly(absolute.location());

                getFileSystem().deleteFile(alias.location());
                assertThat(alias.exists()).isFalse();
                assertThat(absolute.exists()).isFalse();
            }
        }
    }

    @Test
    @Override
    void testListFilesStartingFromConsecutiveSlashesInLocation()
            throws IOException
    {
        // OneLake keeps runs of slashes literal, so the listing reports the doubled-slash path it
        // was given rather than the canonical one ADLS Gen2 collapses to
        try (Closer closer = Closer.create()) {
            createBlob(closer, "level0/level1-file1");
            createBlob(closer, "level0/level1-file2");

            Location doubledSlash = createLocation("level0").appendSuffix("//");
            ImmutableList.Builder<Location> listing = ImmutableList.builder();
            FileIterator iterator = getFileSystem().listFilesStartingFrom(doubledSlash, "level1-file1");
            while (iterator.hasNext()) {
                listing.add(iterator.next().location());
            }

            assertThat(listing.build()).containsExactlyInAnyOrder(
                    Location.of(doubledSlash + "level1-file1"),
                    Location.of(doubledSlash + "level1-file2"));
        }
    }
}
