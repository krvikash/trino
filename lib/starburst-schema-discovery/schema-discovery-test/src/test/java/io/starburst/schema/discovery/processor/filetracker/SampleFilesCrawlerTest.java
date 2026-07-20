/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor.filetracker;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.Util;
import io.starburst.schema.discovery.internal.Errors;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.starburst.schema.discovery.processor.SampleFilesCrawler;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class SampleFilesCrawlerTest
{
    @Test
    public void testRecursiveShouldBreakAfterScanningEnoughFilesForTable()
            throws Exception
    {
        Location recursiveDirectory = Util.testFilePath("recursive");
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1", GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        RecordingFileTrackerDecorator recordingTracker = new RecordingFileTrackerDecorator(new RecursiveDirectoriesFileTracker(recursiveDirectory, new GeneralOptions(optionsMap)));

        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(Util.fileSystem(), recursiveDirectory, optionsMap, directExecutor(), recordingTracker, new Errors());
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        Set<Location> sampleFilePaths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableSet());
        assertThat(sampleFilePaths).hasSize(2); // samples should be 1 per table as specified in options
        // tracker should record only necessary files per table - once enough samples collected, remaining files are skipped
        assertThat(recordingTracker.getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                recursiveDirectory.appendSuffix("/rtable1/"),
                recursiveDirectory.appendSuffix("/rtable1/March-2023/simple2023.csv"),
                recursiveDirectory.appendSuffix("/rtable2/"),
                recursiveDirectory.appendSuffix("/rtable2/April-2023/simple2023.csv")));
    }

    @Test
    public void testPartitionedShouldBreakWhenScanningEnoughFilesForPartition()
            throws Exception
    {
        Result result = scanDirectoryWithMaxFilesConstraint(1, 2);
        // tracker should record only necessary files per partition - once enough samples collected, remaining files are skipped
        assertThat(result.recordingTracker().getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                result.partitionedDirectory(),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-29/"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-29/000000_0"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-30/"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-30/000000_0")));
    }

    @Test
    public void testPartitionedShouldBreakWhenScanningEnoughFilesForPartition2()
            throws Exception
    {
        Result result = scanDirectoryWithMaxFilesConstraint(2, 4);
        // tracker should record only necessary files per partition - once enough samples collected, remaining files are skipped
        assertThat(result.recordingTracker().getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                result.partitionedDirectory(),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-29/"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-29/000000_0"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-29/000000_1"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-30/"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-30/000000_1"),
                result.partitionedDirectory().appendSuffix("/ds=2012-12-30/000000_0")));
    }

    @Test
    public void testNestedPartitionsShouldDiscoverAllLeafPartitions()
            throws Exception
    {
        Location nestedPartitionedDirectory = Util.testFilePath("csv/nested_partitioned_multi_files");
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1"));
        RecordingFileTrackerDecorator recordingTracker = new RecordingFileTrackerDecorator(new PartitionedDiscoveryFileTracker(nestedPartitionedDirectory, new GeneralOptions(optionsMap)));

        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(Util.fileSystem(), nestedPartitionedDirectory, optionsMap, directExecutor(), recordingTracker, new Errors());
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        Set<Location> sampleFilePaths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableSet());
        assertThat(sampleFilePaths).hasSize(2); // both nested partitions should be discovered
        assertThat(sampleFilePaths.stream().map(Location::parentDirectory).collect(toImmutableSet())).hasSize(2); // samples should come from different partitions
        assertThat(sampleFilePaths).anyMatch(location -> location.toString().contains("hour=14"));
        assertThat(sampleFilePaths).anyMatch(location -> location.toString().contains("hour=15"));
        assertThat(recordingTracker.getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                nestedPartitionedDirectory,
                nestedPartitionedDirectory.appendSuffix("/ds=2012-12-29/"),
                nestedPartitionedDirectory.appendSuffix("/ds=2012-12-29/hour=14/000000_0"),
                nestedPartitionedDirectory.appendSuffix("/ds=2012-12-29/hour=15/000000_0")));
    }

    @Test
    public void testRecursiveModeWithNoTopLevelDirectoriesRecordsGlobalError(@TempDir Path tempDir)
            throws Exception
    {
        Location root = Location.of("local://" + tempDir.toAbsolutePath());
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        Errors errors = new Errors();
        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(
                Util.fileSystem(), root, optionsMap, directExecutor(), new RecursiveDirectoriesFileTracker(root, new GeneralOptions(optionsMap)), errors);
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        assertThat(processorPaths).isEmpty();
        // this error has no meaningful table path to attach to (there are no subdirectories at all), so it must
        // be recorded as a schema-level error rather than a table error - assert against build(), which only
        // exposes schema-level errors, so this fails if the crawler mistakenly attaches it to a table path instead
        assertThat(errors.build()).anySatisfy(error -> assertThat(error).contains("no top-level subdirectories"));
        assertThat(errors.buildAll()).anySatisfy(error -> assertThat(error).contains("no top-level subdirectories"));
    }

    private static Result scanDirectoryWithMaxFilesConstraint(int maxSampleFiles, int sampleSize)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        Location partitionedDirectory = Util.testFilePath("csv/partitioned");
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, String.valueOf(maxSampleFiles)));
        RecordingFileTrackerDecorator recordingTracker = new RecordingFileTrackerDecorator(new PartitionedDiscoveryFileTracker(partitionedDirectory, new GeneralOptions(optionsMap)));

        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(Util.fileSystem(), partitionedDirectory, optionsMap, directExecutor(), recordingTracker, new Errors());
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        Set<Location> sampleFilePaths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableSet());
        assertThat(sampleFilePaths).hasSize(sampleSize); // samples should be maxSampleFiles per table(partition) as specified in parameter
        return new Result(partitionedDirectory, recordingTracker);
    }

    private record Result(Location partitionedDirectory, RecordingFileTrackerDecorator recordingTracker) {}
}
