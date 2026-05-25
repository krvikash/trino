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
package io.trino.server;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.ThrowingManagedStatisticsClient;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.scheduler.StableHostAddressProvider;
import io.trino.failuredetector.FailureDetector;
import io.trino.failuredetector.NoOpFailureDetector;
import io.trino.metadata.LanguageFunctionProvider;
import io.trino.metadata.WorkerLanguageFunctionProvider;
import io.trino.server.ui.NoWebUiAuthenticationFilter;
import io.trino.server.ui.WebUiAuthenticationFilter;
import io.trino.spi.connector.ManagedStatisticsClient;
import io.trino.split.ForRemoteSplitsTask;
import io.trino.split.remote.RemoteSplitsTaskManager;
import io.trino.split.remote.RemoteSplitsTaskResource;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;

import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class WorkerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // Install no-op session supplier on workers, since only coordinators create sessions.
        binder.bind(SessionSupplier.class).to(NoOpSessionSupplier.class).in(Scopes.SINGLETON);

        // Install no-op resource group manager on workers, since only coordinators manage resource groups.
        binder.bind(ResourceGroupManager.class).to(NoOpResourceGroupManager.class).in(Scopes.SINGLETON);

        // Install no-op failure detector on workers, since only coordinators need global node selection.
        binder.bind(FailureDetector.class).to(NoOpFailureDetector.class).in(Scopes.SINGLETON);

        // language functions
        binder.bind(WorkerLanguageFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(LanguageFunctionProvider.class).to(WorkerLanguageFunctionProvider.class).in(Scopes.SINGLETON);

        binder.bind(WebUiAuthenticationFilter.class).to(NoWebUiAuthenticationFilter.class).in(Scopes.SINGLETON);

        // managed statistics: not used on workers but binding has to be present for ConnectorContext
        binder.bind(ManagedStatisticsClient.class).to(ThrowingManagedStatisticsClient.class).in(Scopes.SINGLETON);

        // remote splits tasks are served by workers only; the coordinator never selects itself as a candidate
        binder.bind(RemoteSplitsTaskManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(RemoteSplitsTaskResource.class);
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForRemoteSplitsTask.class));
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }

    @Provides
    @Singleton
    public static Optional<StableHostAddressProvider> getRemoteSplitsGenerationAddressProvider()
    {
        // Tasks for remote splits generation are issued from the coordinator only; workers have no ring
        return Optional.empty();
    }

    @Provides
    @Singleton
    @ForRemoteSplitsTask
    public static ExecutorService createRemoteSplitsTaskCreationExecutor()
    {
        // Keeps connector metadata I/O during split-source creation off the shared HTTP serving
        // threads. The bounded queue is the admission control against create storms: a rejected
        // create is answered with REMOTE_SPLITS_TASK_QUEUE_FULL, which the coordinator retries on
        // another worker, rather than queueing past the coordinator's request timeout.
        return new ThreadPoolExecutor(
                8,
                8,
                0,
                SECONDS,
                new LinkedBlockingQueue<>(100),
                daemonThreadsNamed("remote-splits-task-creation-%s"),
                new AbortPolicy());
    }
}
