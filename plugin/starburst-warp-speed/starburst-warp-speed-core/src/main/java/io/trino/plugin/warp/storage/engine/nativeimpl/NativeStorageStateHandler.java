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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.tools.CatalogNameProvider;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageStateHandler
{
    private static final Logger logger = Logger.get(NativeStorageStateHandler.class);

    private final NativeConfig nativeConfig;
    private final CatalogNameProvider catalogNameProvider;
    private final StorageEngine storageEngine;
    private final ShapingLogger shapingLogger;

    boolean alwaysPermanentlyDisableOnError = true;
    boolean storageDisablePermanently = true;
    boolean storageDisableTemporarily = true;
    long storageTemporaryExceptionTimestamp;
    long storageTemporaryExceptionNumTries;

    @Inject
    public NativeStorageStateHandler(
            NativeConfig nativeConfig,
            ExceptionThrower exceptionThrower,
            CatalogNameProvider catalogNameProvider,
            StorageEngine storageEngine,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.nativeConfig = requireNonNull(nativeConfig);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.storageEngine = requireNonNull(storageEngine);
        exceptionThrower.addExceptionConsumer(this::handleErrorCode);
        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    public boolean isStorageAvailable()
    {
        if (!storageEngine.isLoaded() || isStorageDisabledPermanently()) {
            return false;
        }
        if (isStorageDisabledTemporarily()) {
            long configuredExpiryDurationMillis = nativeConfig.getStorageTemporaryExceptionExpiryDuration().toMillis();
            long currentDurationMillis = System.currentTimeMillis() - storageTemporaryExceptionTimestamp;

            // reset storage temp params in case timeout expiry passed
            if (currentDurationMillis > configuredExpiryDurationMillis) {
                enableTemporarily();
                resetTempState();
            }
            else {
                return false;
            }
        }
        return true;
    }

    public synchronized void handleErrorCode(ErrorCodes errorCode)
    {
        logger.debug("[%s] handleErrorCode:: %s", catalogNameProvider.get(), errorCode);

        if (alwaysPermanentlyDisableOnError) {
            storageEngine.shutdown(); // this takes affect for all catalogs as opposed to the disable permanently which is only for this catalog
            shapingLogger.error("storage shutdown for all catalogs error %s", errorCode);
            return;
        }

        if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_PERMANENT_ERROR)) {
            disablePermanently();
            shapingLogger.warn("[%s] storage disabled permanently due to %s", catalogNameProvider.get(), errorCode);
        }
        else if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR) || errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR)) {
            long currentTimeMillis = System.currentTimeMillis();

            // initialize on first temp error
            if (!isStorageDisabledTemporarily()) {
                disableTemporarily();
                shapingLogger.warn("[%s] storage temporary disabled due to %s", catalogNameProvider.get(), errorCode);
            }

            storageTemporaryExceptionNumTries++;
            storageTemporaryExceptionTimestamp = currentTimeMillis;
            shapingLogger.warn("[%s] set storage temporary tries [%d]", catalogNameProvider.get(), storageTemporaryExceptionNumTries);

            // mark as permanent in case too many temp errors
            if (storageTemporaryExceptionNumTries >= nativeConfig.getStorageTemporaryExceptionNumTries()) {
                disablePermanently();
                shapingLogger.warn("[%s] set storage permanent state due to too many temporary errors - %s", catalogNameProvider.get(), errorCode);
            }
        }
    }

    private synchronized void resetTempState()
    {
        storageTemporaryExceptionNumTries = 0;
        storageTemporaryExceptionTimestamp = 0L;
    }

    public boolean isStorageDisabledPermanently()
    {
        return storageDisablePermanently;
    }

    public boolean isStorageDisabledTemporarily()
    {
        return storageDisableTemporarily;
    }

    public void enablePermanently()
    {
        setStorageDisableState(false, null);
    }

    private void disablePermanently()
    {
        setStorageDisableState(true, null);
    }

    public void disableTemporarily()
    {
        setStorageDisableState(null, true);
    }

    public void enableTemporarily()
    {
        setStorageDisableState(null, false);
    }

    public void shutdown()
    {
        disablePermanently();
        logger.warn("[%s] storage disabled permanently due to shutdown", catalogNameProvider.get());
    }

    private synchronized void setStorageDisableState(Boolean disablePermanently, Boolean disableTemporarily)
    {
        if (disablePermanently != null) {
            storageDisablePermanently = disablePermanently;
            if (storageDisablePermanently) {
                storageDisableTemporarily = true;
            }
        }
        else if (disableTemporarily != null) {
            storageDisableTemporarily = disableTemporarily;
            if (!storageDisableTemporarily) {
                shapingLogger.info("[%s] reset storage temporary state due to expiry duration", catalogNameProvider.get());
                resetTempState();
            }
        }
    }
}
