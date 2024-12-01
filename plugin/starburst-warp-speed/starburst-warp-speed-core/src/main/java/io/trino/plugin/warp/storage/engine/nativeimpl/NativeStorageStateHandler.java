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
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.gen.errorcodes.ErrorCodes;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;

import static java.util.Objects.requireNonNull;

@Singleton
public class NativeStorageStateHandler
{
    private final NativeConfig nativeConfig;
    private final ShapingLogger shapingLogger;

    boolean storageDisablePermanently = true;
    boolean storageDisableTemporarily = true;
    long storageTemporaryExceptionTimestamp;
    long storageTemporaryExceptionNumTries;

    @Inject
    public NativeStorageStateHandler(
            NativeConfig nativeConfig,
            ExceptionThrower exceptionThrower,
            GlobalConfig globalConfig)
    {
        this.nativeConfig = requireNonNull(nativeConfig);
        exceptionThrower.addExceptionConsumer(this::handleErrorCode);
        shapingLogger = ShapingLogger.getInstance(
                Logger.get(NativeStorageStateHandler.class),
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    public boolean isStorageAvailable()
    {
        if (isStorageDisabledPermanently()) {
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
        shapingLogger.info("handleErrorCode:: %s", errorCode);

        if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_PERMANENT_ERROR)) {
            disablePermanently();
            shapingLogger.warn("storage disabled permanently due to %s", errorCode);
        }
        else if (errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR) || errorCode.equals(ErrorCodes.ENV_EXCEPTION_STORAGE_TIMEOUT_ERROR)) {
            long currentTimeMillis = System.currentTimeMillis();

            // initialize on first temp error
            if (!isStorageDisabledTemporarily()) {
                disableTemporarily();
                shapingLogger.warn("storage temporary disabled due to %s", errorCode);
            }

            storageTemporaryExceptionNumTries++;
            storageTemporaryExceptionTimestamp = currentTimeMillis;
            shapingLogger.warn("set storage temporary tries [%d]", storageTemporaryExceptionNumTries);

            // mark as permanent in case too many temp errors
            if (storageTemporaryExceptionNumTries >= nativeConfig.getStorageTemporaryExceptionNumTries()) {
                disablePermanently();
                shapingLogger.warn("set storage permanent state due to too many temporary errors - %s", errorCode);
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
                shapingLogger.info("reset storage temporary state due to expiry duration");
                resetTempState();
            }
        }
    }
}
