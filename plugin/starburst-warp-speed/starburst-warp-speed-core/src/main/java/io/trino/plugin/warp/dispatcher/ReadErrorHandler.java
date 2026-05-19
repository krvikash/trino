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

import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.QueryColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.WarpErrorCode.WARP_FAILED_TO_ADD_COLUMN_TO_BUILDER;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_READ_OUT_OF_BOUNDS;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_COLLECT_FAILED;
import static io.trino.plugin.warp.WarpErrorCode.WARP_UNRECOVERABLE_MATCH_FAILED;
import static io.trino.plugin.warp.util.TrinoExceptionMapper.WARP_ERROR_CODE_OFFSET;
import static java.util.Objects.requireNonNull;

@Singleton
public class ReadErrorHandler
{
    private static final Logger logger = Logger.get(ReadErrorHandler.class);
    private final RowGroupDataService rowGroupDataService;
    private final PrintMetricsTimerTask printMetricsTimerTask;

    @Inject
    public ReadErrorHandler(
            RowGroupDataService rowGroupDataService,
            PrintMetricsTimerTask printMetricsTimerTask)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.printMetricsTimerTask = requireNonNull(printMetricsTimerTask);
    }

    void handle(Throwable throwable, RowGroupData failedRowGroupData, QueryContext queryContext)
    {
        if (throwable instanceof TrinoException trinoException) {
            ErrorCode errorCode = trinoException.getErrorCode();
            if (errorCode.equals(WARP_NATIVE_UNRECOVERABLE_ERROR.toErrorCode()) ||
                    errorCode.equals(WARP_UNRECOVERABLE_MATCH_FAILED.toErrorCode()) ||
                    errorCode.equals(WARP_UNRECOVERABLE_COLLECT_FAILED.toErrorCode()) ||
                    errorCode.equals(WARP_NATIVE_UNRECOVERABLE_MATCH_ERROR.toErrorCode())) {
                Set<WarmUpElement> queryContextWarmupElements = getQueryContextWarmupElements(queryContext);
                logger.warn("unrecoverable error: demoting due error code %s rowGroupKey %s queryContextWarmupElements %s", errorCode.getName(), failedRowGroupData.getRowGroupKey(), queryContextWarmupElements);
                rowGroupDataService.markAsFailed(failedRowGroupData.getRowGroupKey(), queryContextWarmupElements, failedRowGroupData.getPartitionKeys());
            }
            else if (errorCode.equals(WARP_NATIVE_READ_OUT_OF_BOUNDS.toErrorCode()) ||
                    errorCode.equals(WARP_FAILED_TO_ADD_COLUMN_TO_BUILDER.toErrorCode())) {
                Set<WarmUpElement> queryContextWarmupElements = getQueryContextWarmupElements(queryContext);
                Collection<WarmUpElement> allAsPermanentlyFailed = queryContextWarmupElements.stream().map(x -> WarmUpElement.builder(x).state(WarmUpElementState.FAILED_PERMANENTLY).build()).collect(Collectors.toSet());
                logger.warn("%s error: marking as failed rowGroupKey %s allAsPermanentlyFailed %s", errorCode, failedRowGroupData.getRowGroupKey(), allAsPermanentlyFailed);
                rowGroupDataService.markAsFailed(failedRowGroupData.getRowGroupKey(), allAsPermanentlyFailed, failedRowGroupData.getPartitionKeys());
            }
            if ((trinoException.getErrorCode().getCode() & WARP_ERROR_CODE_OFFSET) == WARP_ERROR_CODE_OFFSET) {
                // Don't dump metrics when this is not warp error
                StringJoiner joiner = new StringJoiner(", Caused by: ");
                while (throwable != null) {
                    joiner.add(throwable.getMessage());
                    throwable = throwable.getCause();
                }
                printMetricsTimerTask.print(false, Optional.of(joiner.toString()));
            }
        }
    }

    private Set<WarmUpElement> getQueryContextWarmupElements(QueryContext queryContext)
    {
        return Streams.concat(queryContext.getNativeQueryCollectDataList().stream()
                                .map(QueryColumn::getWarmUpElementOptional)
                                .filter(Optional::isPresent)
                                .map(Optional::get),
                        queryContext.getMatchLeavesDFS().stream()
                                .map(QueryColumn::getWarmUpElementOptional)
                                .filter(Optional::isPresent)
                                .map(Optional::get))
                .collect(Collectors.toSet());
    }
}
