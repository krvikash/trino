/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.jfr;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.QueryId;
import jakarta.annotation.PreDestroy;
import jdk.jfr.Configuration;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newInputStream;
import static java.time.Duration.ofMillis;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static jdk.jfr.RecordingState.CLOSED;
import static jdk.jfr.RecordingState.RUNNING;
import static jdk.jfr.RecordingState.STOPPED;

public final class LocalRecordingFactory
        implements FlightRecordingFactory
{
    private static final Logger log = Logger.get(LocalRecordingFactory.class);
    private static final FlightRecorder FLIGHT_RECORDER = FlightRecorder.getFlightRecorder();

    public static final String RECORDING_FILENAME = "recordings/coordinator.jfr";

    private final String nodeId;
    private final Optional<Path> destination;
    private final DataSize maxRecordingSize;

    private final Supplier<Path> fallbackTemporaryPath = memoize(LocalRecordingFactory::generateTemporaryPath);

    @Inject
    public LocalRecordingFactory(InternalNodeManager internalNodeManager, FlightRecorderConfig config)
    {
        this.nodeId = requireNonNull(internalNodeManager, "internalNodeManager is null").getCurrentNode().getNodeIdentifier();
        this.destination = requireNonNull(config, "config is null").getTemporaryDirectory();
        this.maxRecordingSize = config.getMaxRecordingSize();
    }

    @Override
    public Optional<FlightRecording> findOrCreate(QueryId queryId, boolean createIfNeeded)
    {
        Optional<FlightRecording> existingRecording = getExistingRecording(queryId);
        if (createIfNeeded) {
            return existingRecording.or(() -> Optional.of(createNewRecording(queryId)));
        }

        return existingRecording;
    }

    @PreDestroy
    @Override
    public void cleanup()
    {
        ensurePathDeleted(getDestinationPath());
    }

    private Path getRecordingPath(QueryId queryId)
    {
        return getDestinationPath().resolve(queryId.getId()).resolve(nodeId + ".jfr");
    }

    private Optional<FlightRecording> getExistingRecording(QueryId queryId)
    {
        Optional<Recording> running = FLIGHT_RECORDER.getRecordings().stream()
                .filter(recording -> recording.getName().equalsIgnoreCase(getRecordingName(queryId, nodeId)))
                .findFirst();

        if (running.isPresent()) {
            return running.map(recording -> new LocalRunningRecording(queryId, recording));
        }

        return findReadOnlyRecording(queryId, nodeId);
    }

    private Optional<FlightRecording> findReadOnlyRecording(QueryId queryId, String nodeId)
    {
        Path recordingFile = getDestinationPath().resolve(queryId.getId()).resolve(nodeId + ".jfr");
        if (exists(recordingFile)) {
            return Optional.of(new ReadOnlyLocalRecording(recordingFile));
        }
        return Optional.empty();
    }

    private Path getDestinationPath()
    {
        return destination.orElseGet(fallbackTemporaryPath);
    }

    private static String getRecordingName(QueryId queryId, String nodeId)
    {
        return "JavaFlightRecording{queryId=%s;nodeId=%s}".formatted(queryId.getId(), nodeId);
    }

    private FlightRecording createNewRecording(QueryId queryId)
    {
        String recordingName = getRecordingName(queryId, nodeId);
        Path recordingPath = getRecordingPath(queryId);
        ensurePathExists(recordingPath.getParent());

        try {
            Configuration configuration = Configuration.getConfiguration("default");
            Recording recording = new Recording(configuration);
            recording.setName(recordingName);
            recording.setDestination(recordingPath);
            recording.setDumpOnExit(true);
            recording.setToDisk(true);
            recording.setMaxSize(maxRecordingSize.toBytes());

            disableSensitiveEvents(recording);
            return new LocalRunningRecording(queryId, recording);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static void disableSensitiveEvents(Recording recording)
    {
        recording.disable("jdk.InitialEnvironmentVariable"); // could contain secrets
        recording.disable("jdk.InitialSystemProperty"); // could contain secrets
        recording.disable("jdk.JVMInformation"); // could contain secrets
    }

    private static void ensurePathExists(Path dir)
    {
        if (exists(dir) && isDirectory(dir)) {
            return;
        }

        try {
            Files.createDirectories(dir);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void ensurePathDeleted(Path dir)
    {
        if (exists(dir)) {
            try {
                deleteRecursively(dir, ALLOW_INSECURE);
            }
            catch (IOException e) {
                log.warn(e, "Could not remove temporary path: %s", dir);
            }
        }
    }

    private static DataSize fileSize(Path path)
    {
        try {
            return DataSize.ofBytes(Files.size(path)).succinct();
        }
        catch (IOException e) {
            return DataSize.ofBytes(0);
        }
    }

    static Path generateTemporaryPath()
    {
        try {
            Path tempDirectory = createTempDirectory("query-troubleshooting");
            log.info("Created temporary directory for JFR files: %s", tempDirectory);
            return tempDirectory;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static final class LocalRunningRecording
            implements FlightRecording
    {
        private final QueryId queryId;
        private final Recording recording;
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final AtomicBoolean removed = new AtomicBoolean(false);

        private LocalRunningRecording(QueryId queryId, Recording recording)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.recording = requireNonNull(recording, "recording is null");
            this.started.set(recording.getState() == RUNNING);
            this.finished.set(recording.getState() == STOPPED);
            this.removed.set(recording.getState() == CLOSED);
        }

        @Override
        public FlightRecording start()
        {
            if (started.compareAndSet(false, true)) {
                recording.start();
                log.info("Started %s", this);
            }

            return this;
        }

        @Override
        public void finish()
        {
            if (!finished.compareAndSet(false, true)) {
                return;
            }

            if (recording.getState() == RecordingState.RUNNING) {
                recording.stop();
            }
            log.info("Finished %s", this);
        }

        @Override
        public void remove()
        {
            finish();
            if (!removed.compareAndSet(false, true)) {
                return;
            }

            // Releases all resources maintained by the recording
            recording.close();

            if (exists(recording.getDestination())) {
                try {
                    delete(recording.getDestination());
                    verify(recording.getDestination().getParent().endsWith(queryId.getId()), "Parent path does not contain query id");
                    ensurePathDeleted(recording.getDestination().getParent());
                    log.info("Removed %s", this);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        @Override
        public Map<String, InputStream> getInputStreams()
        {
            verify(!removed.get(), "Cannot access troubleshooting data as it is already removed");

            // Stop forces recording to be dumped to the disk
            finish();
            try {
                return ImmutableMap.of(RECORDING_FILENAME, newInputStream(recording.getDestination()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Duration getDuration()
        {
            Instant endTime = firstNonNull(recording.getStopTime(), Instant.now());
            return ofMillis(recording.getStartTime().until(endTime, MILLIS));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", recording.getId())
                    .add("currentState", recording.getState().name())
                    .add("duration", getDuration())
                    .add("size", fileSize(recording.getDestination()))
                    .add("path", recording.getDestination())
                    .toString();
        }
    }

    static final class ReadOnlyLocalRecording
            implements FlightRecording
    {
        private final Path recordingPath;

        private ReadOnlyLocalRecording(Path recordingPath)
        {
            this.recordingPath = requireNonNull(recordingPath, "recordingPath is null");
            verify(exists(recordingPath), "Recording file %s does not exist", recordingPath);
        }

        @Override
        public FlightRecording start()
        {
            return this;
        }

        @Override
        public void finish()
        {
            // noop
        }

        @Override
        public void remove()
        {
            try {
                deleteIfExists(recordingPath);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            log.info("Removed %s", this);
        }

        @Override
        public Map<String, InputStream> getInputStreams()
        {
            try {
                return ImmutableMap.of(RECORDING_FILENAME, newInputStream(recordingPath));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("path", recordingPath)
                    .add("size", fileSize(recordingPath))
                    .toString();
        }
    }
}
