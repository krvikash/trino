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
package io.trino.plugin.warp.dispatcher;

import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.log.ShapingLogger;
import org.apache.commons.lang3.function.TriConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RowGroupCloseHandler
        implements TriConsumer<RowGroupData, String, ShapingLogger>
{
    private static final io.airlift.log.Logger logger = Logger.get(RowGroupCloseHandler.class);

    private final AtomicBoolean handled = new AtomicBoolean();
    private final List<String> callers = new ArrayList<>();

    @Override
    public void accept(RowGroupData rowGroupData, String caller, ShapingLogger shapingLogger)
    {
        if (!handled.getAndSet(true)) {
            rowGroupData.getLock().readUnLock();
        }
        else {
            if (shapingLogger != null) {
                shapingLogger.warn("already called & handled by this close handled for rowGroup[%s], previous callers %s", rowGroupData.getRowGroupKey(), callers);
            }
            else {
                logger.warn("already called & handled by this close handled for rowGroup[%s], previous callers %s", rowGroupData.getRowGroupKey(), callers);
            }
        }
        callers.add(caller);
    }
}
