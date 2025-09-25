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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.metastore.BaseVendedFileSystemCredentials;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.metastore.VendedCredentialsProvider;
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class UnityVendedCredentialsProvider
        implements VendedCredentialsProvider
{
    private final UnityMetastore unityMetastore;

    @Inject
    public UnityVendedCredentialsProvider(UnityHiveMetastoreFactory metastoreFactory)
    {
        this.unityMetastore = (UnityMetastore) requireNonNull(metastoreFactory, "metastoreFactory is null").createMetastore(Optional.empty());
    }

    @Override
    public VendedCredentialsHandle getFreshCredentials(VendedCredentialsHandle handle)
    {
        if (handle.vendedCredentials().map(FileSystemCredentials::isValid).orElse(false)) {
            return handle;
        }

        Optional<String> tableId = handle.tableId();
        TemporaryCredentials temporaryCredentials;
        if (handle.catalogOwned()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ_WRITE);
        }
        else if (handle.managed()) {
            temporaryCredentials = unityMetastore.getTemporaryTableCredentials(tableId.orElseThrow(), TableOperation.READ);
        }
        else { // external table
            temporaryCredentials = unityMetastore.getTemporaryPathCredentials(handle.tableLocation(), PathOperation.PATH_READ_WRITE);
        }

        FileSystemCredentials credentials = BaseVendedFileSystemCredentials.fromTemporaryCredentials(temporaryCredentials);
        verify(credentials.isValid(), "vended credentials is not valid");
        return handle.withVendedCredentials(credentials);
    }
}
