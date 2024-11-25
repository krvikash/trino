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
package io.trino.plugin.warp.extension.execution.warmup;

import com.google.inject.Inject;
import io.trino.plugin.warp.annotation.Audit;
import io.trino.plugin.warp.api.warmup.WarmupColRuleData;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupRuleProvider;
import io.trino.plugin.warp.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.warmup.WarmupRuleApiMapper;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

import static io.trino.plugin.warp.extension.execution.warmup.WorkerCacheMgrWarmupTask.WORKER_WARMUP_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false, cacheMgr = true, connector = false)
@Path(WORKER_WARMUP_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerCacheMgrWarmupTask
        implements TaskResource
{
    public static final String WORKER_WARMUP_PATH = "worker-cache-manager-warmup";
    public static final String TASK_NAME_FETCH = "run-fetcher";

    private final WarmupRuleFetcher<CacheManagerRule> warmupRuleFetcher;
    private final WarmupRuleProvider warmupRuleProvider;

    @Inject
    public WorkerCacheMgrWarmupTask(
            WarmupRuleFetcher<CacheManagerRule> warmupRuleFetcher,
            WarmupRuleProvider warmupRuleProvider)
    {
        this.warmupRuleFetcher = requireNonNull(warmupRuleFetcher);
        this.warmupRuleProvider = requireNonNull(warmupRuleProvider);
    }

    @Path(TASK_NAME_FETCH)
    @GET
    @Audit
    public List<WarmupColRuleData> fetch()
    {
        warmupRuleFetcher.fetch();
        return warmupRuleProvider.getAll()
                .stream()
                .map(WarmupRuleApiMapper::fromModel)
                .toList();
    }
}
