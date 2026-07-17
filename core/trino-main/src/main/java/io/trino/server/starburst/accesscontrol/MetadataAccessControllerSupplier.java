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
package io.trino.server.starburst.accesscontrol;

import io.trino.spi.security.Identity;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MetadataAccessControllerSupplier
        implements GalaxyAccessControllerSupplier
{
    public static final String TRANSACTION_ID_KEY = "galaxy-metadata-transaction-id";

    private final Map<TransactionId, GalaxyAccessControllerApi> controllers = new ConcurrentHashMap<>();

    @Override
    public GalaxyAccessControllerApi apply(Identity identity)
    {
        return extractTransactionId(identity)
                .map(controllers::get)
                .orElseThrow(() -> new NullPointerException("controller is null"));
    }

    public static Optional<TransactionId> extractTransactionId(Identity identity)
    {
        return Optional.ofNullable(identity.getExtraCredentials().get(TRANSACTION_ID_KEY))
                .map(TransactionId::valueOf);
    }

    public void addController(TransactionId transactionId, GalaxyAccessControllerApi controller)
    {
        controllers.put(transactionId, controller);
    }

    public void removeController(TransactionId transactionId)
    {
        controllers.remove(transactionId);
    }
}
