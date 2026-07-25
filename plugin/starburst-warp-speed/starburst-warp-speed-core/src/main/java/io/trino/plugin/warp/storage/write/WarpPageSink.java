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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.util.ExceptionUtils;
import io.trino.plugin.warp.warmup.exceptions.MaxRowsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;

import static java.util.Objects.requireNonNull;

public class WarpPageSink
        implements PageSink
{
    private final ShapingLogger shapingLogger;
    private final StorageWriterService storageWriterService;
    private final StorageWriterSplitConfig storageWriterSplitConfig;
    private boolean writerOpened; // represents a writer(native) open
    private WarmUpElement abortedWarmupElement;

    private StorageWriterContext storageWriterContext;

    public WarpPageSink(
            StorageWriterService storageWriterService,
            StorageWriterSplitConfig storageWriterSplitConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.storageWriterService = storageWriterService;
        this.storageWriterSplitConfig = storageWriterSplitConfig;
        shapingLogger = requireNonNull(shapingLoggerFactory).getInstance(WarpPageSink.class);
    }

    @Override
    public void open(long[] fileCookieParams, int startOffset, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        // now create the native tx
        try {
            WriteOpenResult writeOpenResult = storageWriterService.open(fileCookieParams, startOffset, storageWriterSplitConfig, warmupElementWriteMetadata);
            storageWriterContext = writeOpenResult.storageWriterContext();
            writerOpened = true;
        }
        catch (Exception e) {
            shapingLogger.error("we create failed %s", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean appendPage(SourcePage page, int totalRecords)
    {
        try {
            if ((long) totalRecords + (long) page.getPositionCount() >= Integer.MAX_VALUE) {
                throw new MaxRowsException();
            }
            return storageWriterService.appendPage(page, storageWriterContext);
        }
        catch (TrinoException te) {
            // skip log if cause is 'Connection pool shut down'
            if (!ExceptionUtils.isCausedBy(te, IllegalStateException.class)) {
                shapingLogger.error(te, "appendPage thrown a TrinoException - aborting. storageWriterContext=%s", storageWriterContext);
            }
            abort(ExceptionThrower.isNativeException(te));
            return false;
        }
        catch (MaxRowsException maxRowsException) {
            abort(false);
            abortedWarmupElement = WarmUpElement.builder(abortedWarmupElement).state(new WarmUpElementState(maxRowsException.getState())).build();
            return false;
        }
        catch (Exception e) { // in case of exception the writer has aborted the tx internally already, we need to release it now
            shapingLogger.error(e, "appendPage thrown an exception - aborting. storageWriterContext=%s", storageWriterContext);
            abort(false);
            return false;
        }
    }

    @Override
    public WarmSinkResult close(int totalRecords)
    {
        try {
            if (!writerOpened) {
                return new WarmSinkResult(abortedWarmupElement, 0);
            }
            return storageWriterService.close(totalRecords, storageWriterSplitConfig, storageWriterContext);
        }
        finally {
            writerOpened = false;
        }
    }

    @Override
    public void abort(boolean nativeThrowed)
    {
        try {
            if (writerOpened) {
                abortedWarmupElement = storageWriterService.abort(nativeThrowed, storageWriterContext, storageWriterSplitConfig);
            }
        }
        finally {
            writerOpened = false;
        }
    }
}
