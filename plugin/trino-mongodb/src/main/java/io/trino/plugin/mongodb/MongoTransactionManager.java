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
package io.trino.plugin.mongodb;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class MongoTransactionManager
{
    private final ConcurrentMap<ConnectorTransactionHandle, MemoizedMetadata> transactions = new ConcurrentHashMap<>();
    private final MongoMetadataFactory metadataFactory;

    @Inject
    public MongoTransactionManager(MongoMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        MongoTransactionHandle transaction = new MongoTransactionHandle();
        transactions.put(transaction, new MemoizedMetadata());
        return transaction;
    }

    public MongoMetadata getMetadata(ConnectorTransactionHandle transaction, ConnectorIdentity connectorIdentity)
    {
        MemoizedMetadata memoizedMetadata = transactions.get(transaction);
        checkArgument(memoizedMetadata != null, "no such transaction: %s", transaction);
        return memoizedMetadata.get(connectorIdentity);
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata memoizedMetadata = transactions.remove(transaction);
        checkArgument(memoizedMetadata != null, "no such transaction: %s", transaction);
        memoizedMetadata.optionalGet().ifPresent(MongoMetadata::rollback);
    }

    // From HiveTransactionManager
    private class MemoizedMetadata
    {
        @GuardedBy("this")
        private MongoMetadata metadata;

        public synchronized Optional<MongoMetadata> optionalGet()
        {
            return Optional.ofNullable(metadata);
        }

        public synchronized MongoMetadata get(ConnectorIdentity identity)
        {
            if (metadata == null) {
                metadata = metadataFactory.create(identity);
            }
            return metadata;
        }
    }
}
