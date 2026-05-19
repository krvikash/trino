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
package io.trino.memory;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import oshi.software.os.linux.LinuxOperatingSystem;
import oshi.util.FileUtil;
import oshi.util.ParseUtil;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oshi.util.platform.linux.ProcPath.MEMINFO;

public class RssMonitor
{
    private static final Logger log = Logger.get(RssMonitor.class);
    private ScheduledExecutorService scheduler;
    Optional<Duration> rssMemorySampleInterval;
    private long rssMemory;
    private FileCacheMemory fileCacheMemory = new FileCacheMemory(0, 0);
    private volatile boolean meminfoCorrectlyFormatted = true;

    @Inject
    public RssMonitor(NodeMemoryConfig config)
    {
        this.rssMemorySampleInterval = config.getRssMemorySampleInterval();
    }

    @PostConstruct
    public void start()
    {
        // Only monitor RSS memory if the interval is at least 5 seconds.
        if (rssMemorySampleInterval.isPresent() && rssMemorySampleInterval.get().toMillis() >= 5000) {
            scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("run-in-native-process-%s"));
            scheduler.scheduleAtFixedRate(
                    () -> executeAsExternalProcess(),
                    rssMemorySampleInterval.get().toMillis(),
                    rssMemorySampleInterval.get().toMillis(),
                    MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    public void executeAsExternalProcess()
    {
        SystemInfo systemInfo = new SystemInfo();
        OperatingSystem os = systemInfo.getOperatingSystem();
        OSProcess process = os.getCurrentProcess();
        rssMemory = process.getResidentSetSize();
        if (os instanceof LinuxOperatingSystem && meminfoCorrectlyFormatted) {
            fileCacheMemory = getRSS();
        }
    }

    record FileCacheMemory(long active, long inactive) {}

    private FileCacheMemory getRSS()
    {
        Optional<Long> activeFile = Optional.empty();
        Optional<Long> inactiveFile = Optional.empty();

        List<String> procMemInfo = FileUtil.readFile(MEMINFO);
        for (String checkLine : procMemInfo) {
            String[] memorySplit = ParseUtil.whitespaces.split(checkLine, 2);
            if (memorySplit.length > 1) {
                switch (memorySplit[0]) {
                    case "Active(file):" -> activeFile = Optional.of(ParseUtil.parseDecimalMemorySizeToBinary(memorySplit[1]));
                    case "Inactive(file):" -> inactiveFile = Optional.of(ParseUtil.parseDecimalMemorySizeToBinary(memorySplit[1]));
                    default -> {
                        // do nothing with other lines
                    }
                }
            }
        }
        if (activeFile.isPresent() && inactiveFile.isPresent()) {
            return new FileCacheMemory(activeFile.get(), inactiveFile.get());
        }
        log.error("Could not read Active(file) and Inactive(file) from /proc/meminfo");
        meminfoCorrectlyFormatted = false;
        return new FileCacheMemory(0, 0);
    }

    @Managed
    public long getRssMemory()
    {
        return rssMemory;
    }

    @Managed
    public long getActiveFileCacheMemory()
    {
        return fileCacheMemory.active();
    }

    @Managed
    public long getInactiveFileCacheMemory()
    {
        return fileCacheMemory.inactive();
    }
}
