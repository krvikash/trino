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
package io.trino.plugin.warp.storage.read;

import io.airlift.log.Logger;

public class ReadTimeMeasurement
{
    private static final Logger logger = Logger.get(ReadTimeMeasurement.class);
    private static final long TIME_REPORT_INTERVAL_NANOS = 1000000000L * 60; // 1 minute

    private long lastReportTime;
    private long totalRunTime;
    private long minRoundTime;
    private long maxRoundTime;

    ReadTimeMeasurement()
    {
        this.lastReportTime = System.nanoTime();
        this.minRoundTime = Long.MAX_VALUE;
    }

    public long getStartTime()
    {
        return System.nanoTime();
    }

    public void updateRuntimeMeasurements(long startTime, QueryArgs queryArgs)
    {
        long outTime = System.nanoTime();
        long roundTime = outTime - startTime;
        if (roundTime < minRoundTime) {
            minRoundTime = roundTime;
        }
        if (roundTime > maxRoundTime) {
            maxRoundTime = roundTime;
        }
        totalRunTime += roundTime;
        if (outTime - lastReportTime >= TIME_REPORT_INTERVAL_NANOS) {
            logger.info("totalRunTime %d minRoundTime %d maxRoundTime %d processed %d chunks out of %d",
                    totalRunTime, minRoundTime, maxRoundTime, queryArgs.chunksQueue().getTotalNumChunks(), queryArgs.numChunks());
            lastReportTime = outTime;
        }
    }
}
