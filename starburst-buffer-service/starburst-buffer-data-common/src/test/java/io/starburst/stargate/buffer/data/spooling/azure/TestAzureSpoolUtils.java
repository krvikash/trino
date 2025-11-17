/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.AzureUriInfo;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.getAzureUriInfo;
import static io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.keyFromUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAzureSpoolUtils
{
    @Test
    void testGetAzureUriInfoWithNormalUri()
    {
        // URI with container as userInfo and host
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/path/to/dir");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("path/to/dir");
    }

    @Test
    void testGetAzureUriInfoWithComplexPath()
    {
        // URI with complex nested path
        URI uri = URI.create("abfs://mycontainer@mystorageaccount.dfs.core.windows.net/some/deep/nested/path");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("mystorageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("mycontainer");
        assertThat(azureUriInfo.path()).isEqualTo("some/deep/nested/path");
    }

    @Test
    void testGetAzureUriInfoWithNoPath()
    {
        // URI with just container and host, no path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("");
    }

    @Test
    void testGetAzureUriInfoWithLeadingSlashes()
    {
        // URI with leading slashes in path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net///path/to/dir");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("path/to/dir");
    }

    @Test
    void testGetAzureUriInfoWithTrailingSlashes()
    {
        // URI with trailing slashes in path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/path/to/dir///");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("path/to/dir");
    }

    @Test
    void testGetAzureUriInfoWithLeadingAndTrailingSlashes()
    {
        // URI with both leading and trailing slashes
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net///some//path///");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("some//path");
    }

    @Test
    void testGetAzureUriInfoWithEmptyPath()
    {
        // URI with empty path (just slash)
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("");
    }

    @Test
    void testGetAzureUriInfoWithMissingHost()
    {
        // URI without host should throw exception
        URI uri = URI.create("abfs://container/path/to/dir");
        assertThatThrownBy(() -> getAzureUriInfo(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid abfs URI");
    }

    @Test
    void testGetAzureUriInfoWithMissingContainerName()
    {
        // URI without container (no userInfo) should throw exception
        URI uri = URI.create("abfs://storageaccount.dfs.core.windows.net/path/to/dir");
        assertThatThrownBy(() -> getAzureUriInfo(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid abfs URI");
    }

    @Test
    void testGetAzureUriInfoWithSpecialCharactersInPath()
    {
        // URI with special characters in path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/path/with-dashes_and_underscores/dir");
        AzureUriInfo azureUriInfo = getAzureUriInfo(uri);
        assertThat(azureUriInfo.hostName()).isEqualTo("storageaccount.dfs.core.windows.net");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("path/with-dashes_and_underscores/dir");
    }

    @Test
    void testKeyFromUriWithNormalPath()
    {
        // Normal URI with path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/path/to/dir");
        String key = keyFromUri(uri);
        assertThat(key).isEqualTo("path/to/dir");
    }

    @Test
    void testKeyFromUriWithLeadingSlash()
    {
        // URI with leading slash in path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/path/to/dir");
        String key = keyFromUri(uri);
        assertThat(key).isEqualTo("path/to/dir");
    }

    @Test
    void testKeyFromUriWithMultipleLeadingSlashes()
    {
        // URI with multiple leading slashes
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net///path/to/dir");
        String key = keyFromUri(uri);
        assertThat(key).isEqualTo("//path/to/dir");
    }

    @Test
    void testKeyFromUriWithSingleFile()
    {
        // URI with single file name
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/dir");
        String key = keyFromUri(uri);
        assertThat(key).isEqualTo("dir");
    }

    @Test
    void testKeyFromUriWithRelativeUri()
    {
        // Relative URI should throw exception
        URI uri = URI.create("path/to/dir");
        assertThatThrownBy(() -> keyFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Uri is not absolute");
    }

    @Test
    void testKeyFromUriWithEmptyPath()
    {
        // URI with no path should throw exception
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net");
        assertThatThrownBy(() -> keyFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid abfs URI passed to keyFromUri");
    }

    @Test
    void testKeyFromUriWithOnlySlash()
    {
        // URI with only slash as path should throw exception
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/");
        assertThatThrownBy(() -> keyFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid abfs URI passed to keyFromUri");
    }

    @Test
    void testKeyFromUriWithDeepPath()
    {
        // URI with deep nested path
        URI uri = URI.create("abfs://container@storageaccount.dfs.core.windows.net/very/deep/nested/path/to/dir");
        String key = keyFromUri(uri);
        assertThat(key).isEqualTo("very/deep/nested/path/to/dir");
    }

    @Test
    void testAzureUriInfoRecordNullChecks()
    {
        // Test that the record validates non-null fields
        assertThatThrownBy(() -> new AzureUriInfo(null, "container", "path"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hostName is null");

        assertThatThrownBy(() -> new AzureUriInfo("host", null, "path"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("containerName is null");

        assertThatThrownBy(() -> new AzureUriInfo("host", "container", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path is null");
    }

    @Test
    void testAzureUriInfoRecordCreation()
    {
        // Test that the record can be created with valid values
        AzureUriInfo azureUriInfo = new AzureUriInfo("hostname", "container", "path");
        assertThat(azureUriInfo.hostName()).isEqualTo("hostname");
        assertThat(azureUriInfo.containerName()).isEqualTo("container");
        assertThat(azureUriInfo.path()).isEqualTo("path");
    }
}
