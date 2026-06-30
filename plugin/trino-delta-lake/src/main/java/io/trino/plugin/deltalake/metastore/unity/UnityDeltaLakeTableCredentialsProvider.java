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
import io.trino.plugin.hive.metastore.unity.UnityHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class UnityDeltaLakeTableCredentialsProvider
        extends AbstractUnityDeltaLakeTableCredentialsProvider
{
    private final UnityMetastore unityMetastore;

    @Inject
    public UnityDeltaLakeTableCredentialsProvider(UnityHiveMetastoreFactory metastoreFactory)
    {
        this.unityMetastore = (UnityMetastore) requireNonNull(metastoreFactory, "metastoreFactory is null").createMetastore(Optional.empty());
    }

    @Override
    protected UnityMetastore getUnityMetastore(ConnectorIdentity identity)
    {
        return unityMetastore;
    }
}
