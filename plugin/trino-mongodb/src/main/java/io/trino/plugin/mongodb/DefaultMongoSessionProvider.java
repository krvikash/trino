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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.mongo.v3_1.MongoTelemetry;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DefaultMongoSessionProvider
        implements MongoSessionProvider
{
    private final MongoSession mongoSession;

    @Inject
    public DefaultMongoSessionProvider(
            TypeManager typeManager,
            MongoClientConfig config,
            Set<MongoClientSettingConfigurator> configurators,
            OpenTelemetry openTelemetry)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(config, "config is null");
        requireNonNull(configurators, "configurators is null");
        requireNonNull(openTelemetry, "openTelemetry is null");

        Supplier<MongoClient> client = Suppliers.memoize(() -> {
            MongoClientSettings.Builder options = MongoClientSettings.builder();
            configurators.forEach(configurator -> configurator.configure(options));
            options.addCommandListener(MongoTelemetry.builder(openTelemetry).build().createCommandListener());
            return MongoClients.create(options.build());
        });

        this.mongoSession = new MongoSession(
                typeManager,
                client,
                config);
    }

    @Override
    public MongoSession getMongoSession(ConnectorIdentity connectorIdentity)
    {
        return mongoSession;
    }

    @PreDestroy
    public void close()
    {
        mongoSession.close();
    }
}
