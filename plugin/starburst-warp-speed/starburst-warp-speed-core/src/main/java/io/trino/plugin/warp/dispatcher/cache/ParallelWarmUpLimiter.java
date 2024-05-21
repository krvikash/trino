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
package io.trino.plugin.warp.dispatcher.cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.GlobalConfig;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

@Singleton
public class ParallelWarmUpLimiter
{
    private final int cacheManagerMaxParallelWarmupElements;
    private final AtomicInteger currentlyUsedWarmUpElements;

    @Inject
    public ParallelWarmUpLimiter(GlobalConfig globalConfig)
    {
        this.cacheManagerMaxParallelWarmupElements = requireNonNull(globalConfig).getCacheManagerMaxParallelWarmupElements();
        this.currentlyUsedWarmUpElements = new AtomicInteger(0);
    }

    public void release(int warmupElementsInUse)
    {
        currentlyUsedWarmUpElements.addAndGet(-1 * warmupElementsInUse);
    }

    public boolean tryToUse(int warmupElementsToUse)
    {
        if (currentlyUsedWarmUpElements.addAndGet(warmupElementsToUse) > cacheManagerMaxParallelWarmupElements) {
            release(warmupElementsToUse);
            return false;
        }
        return true;
    }
}
