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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import io.trino.filesystem.Location;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class AzureLocation
{
    private static final String INVALID_ABFS_LOCATION_MESSAGE = "Invalid Azure ABFS location. Expected form is 'abfs[s]://[<containerName>@]<accountName>.dfs.<endpoint>/<filePath>': %s";
    private static final String INVALID_WASB_LOCATION_MESSAGE = "Invalid Azure WASB location. Expected form is 'wasb[s]://[<containerName>@]<accountName>.blob.<endpoint>/<filePath>': %s";

    // Microsoft Fabric serves OneLake from its own endpoint rather than core.windows.net
    // https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api
    private static final String ONELAKE_ENDPOINT = "fabric.microsoft.com";

    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
    private static final CharMatcher CONTAINER_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9')).or(CharMatcher.is('-'));
    private static final CharMatcher STORAGE_ACCOUNT_VALID_CHARACTERS = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('0', '9')).or(CharMatcher.is('-'));

    private final Location location;
    private final String scheme;
    private final String serviceLabel;
    private final String account;
    private final String endpoint;

    /**
     * Creates a new location based on the endpoint, storage account, container and blob path parsed from the location.
     * <p>
     * Locations use the
     * <a href="https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri">ABFS URI</a> syntax:
     * <pre>{@code abfs[s]://<container-name>@<storage-account-name>.dfs.<endpoint>/<blob_path>}</pre>
     */
    public AzureLocation(Location location)
    {
        this.location = requireNonNull(location, "location is null");
        // wasb and wasbs are also supported but not documented
        scheme = location.scheme().orElseThrow(() -> new IllegalArgumentException(String.format(INVALID_ABFS_LOCATION_MESSAGE, location)));
        String invalidLocationMessage;
        // abfs[s] hosts address the dfs service, wasb[s] hosts address the blob service
        if ("abfs".equals(scheme) || "abfss".equals(scheme)) {
            invalidLocationMessage = INVALID_ABFS_LOCATION_MESSAGE;
            this.serviceLabel = "dfs";
        }
        else if ("wasb".equals(scheme) || "wasbs".equals(scheme)) {
            invalidLocationMessage = INVALID_WASB_LOCATION_MESSAGE;
            this.serviceLabel = "blob";
        }
        else {
            // only mention abfs in error message as the other forms are deprecated
            throw new IllegalArgumentException(String.format(INVALID_ABFS_LOCATION_MESSAGE, location));
        }

        // container is interpolated into the URL path, so perform extra checks
        location.userInfo().ifPresent(container -> {
            checkArgument(!container.isEmpty(), invalidLocationMessage, location);
            checkArgument(
                    CONTAINER_VALID_CHARACTERS.matchesAllOf(container),
                    "Invalid Azure storage container name. Valid characters are 'a-z', '0-9', and '-': %s",
                    location);
            checkArgument(
                    !container.startsWith("-") && !container.endsWith("-"),
                    "Invalid Azure storage container name. Cannot start or end with a hyphen: %s",
                    location);
            checkArgument(
                    !container.contains("--"),
                    "Invalid Azure storage container name. Cannot contain consecutive hyphens: %s",
                    location);
        });

        checkArgument(location.host().isPresent(), invalidLocationMessage, location);
        String host = location.host().get();
        String serviceSeparator = ".%s.".formatted(serviceLabel);
        int serviceSplit = host.indexOf(serviceSeparator);
        checkArgument(serviceSplit > 0, invalidLocationMessage, location);

        this.account = host.substring(0, serviceSplit);
        // endpoint does not include the service label
        this.endpoint = host.substring(serviceSplit + serviceSeparator.length());
        checkArgument(!endpoint.isEmpty(), invalidLocationMessage, location);

        // storage account is interpolated into URL host name, so perform extra checks. OneLake
        // workspace FQDNs may contain more than one label ahead of the service label.
        for (String label : Splitter.on('.').split(account)) {
            checkArgument(!label.isEmpty() && STORAGE_ACCOUNT_VALID_CHARACTERS.matchesAllOf(label),
                    "Invalid Azure storage account name. Valid characters are 'a-z' and '0-9': %s",
                    location);
        }
    }

    public Location location()
    {
        return location;
    }

    public Optional<String> container()
    {
        return location.userInfo();
    }

    public String account()
    {
        return account;
    }

    public String endpoint()
    {
        return endpoint;
    }

    public String path()
    {
        return location.path();
    }

    public String directoryPath()
    {
        String path = location.path();
        if (!path.isEmpty() && !path.endsWith("/")) {
            path += "/";
        }
        return path;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s%s.%s.%s/".formatted(
                scheme,
                container().map(container -> container + "@").orElse(""),
                account(),
                serviceLabel,
                endpoint));
    }

    public boolean isOneLake()
    {
        return ONELAKE_ENDPOINT.equals(endpoint);
    }
}
