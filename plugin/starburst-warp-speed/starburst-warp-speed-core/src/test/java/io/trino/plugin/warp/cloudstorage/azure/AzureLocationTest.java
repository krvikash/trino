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
package io.trino.plugin.warp.cloudstorage.azure;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AzureLocationTest
{
    @Test
    void testAdlsLocation()
    {
        AzureLocation location = AzureLocation.from("abfss://container@account.dfs.core.windows.net/some/path");
        assertThat(location.account()).isEqualTo("account");
        assertThat(location.endpoint()).isEqualTo("core.windows.net");
        assertThat(location.container()).isEqualTo(Optional.of("container"));
        assertThat(location.path()).isEqualTo("some/path");
    }

    @Test
    void testOneLakeRegionalLocation()
    {
        AzureLocation location = AzureLocation.from("abfss://workspace@eastus-onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables/orders/data");
        assertThat(location.account()).isEqualTo("eastus-onelake");
        assertThat(location.endpoint()).isEqualTo("fabric.microsoft.com");
        assertThat(location.container()).isEqualTo(Optional.of("workspace"));
        assertThat(location.path()).isEqualTo("sales.Lakehouse/Tables/orders/data");
    }

    @Test
    void testOneLakeGlobalLocation()
    {
        AzureLocation location = AzureLocation.from("abfss://workspace@onelake.dfs.fabric.microsoft.com/sales.Lakehouse/Tables");
        assertThat(location.account()).isEqualTo("onelake");
        assertThat(location.endpoint()).isEqualTo("fabric.microsoft.com");
    }

    @Test
    void testOneLakeWorkspaceFqdnLocation()
    {
        AzureLocation location = AzureLocation.from("abfss://workspace@myworkspace.z54.dfs.fabric.microsoft.com/sales.Lakehouse");
        assertThat(location.account()).isEqualTo("myworkspace.z54");
        assertThat(location.endpoint()).isEqualTo("fabric.microsoft.com");
    }

    @Test
    void testInvalidLocation()
    {
        assertThatThrownBy(() -> AzureLocation.from("abfss://workspace@onelake.blob.fabric.microsoft.com/sales.Lakehouse"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Azure location");
        assertThatThrownBy(() -> AzureLocation.from("abfss://account.core.windows.net/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Azure location");
        assertThatThrownBy(() -> AzureLocation.from("abfss://account.dfs./some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Azure location");
        assertThatThrownBy(() -> AzureLocation.from("abfss://ACCOUNT.dfs.core.windows.net/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Azure storage account name");
    }
}
