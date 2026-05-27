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
package io.trino.tests.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.abs;
import static java.util.Objects.requireNonNull;

/**
 * Combines an RMM allocation log with GPU allocation trace stacks to show
 * what code paths consumed the most GPU memory at peak.
 * <p>
 * The two logs come from independent sources: the RMM log (CSV from
 * {@code Rmm.logTo()}) records every allocate/free with pointer, size,
 * timestamp, and native thread ID; the trace log ({@code AllocationTraceHandler})
 * captures Java stack traces for allocations above a size threshold, also
 * with timestamp, native thread ID, and size.
 * <p>
 * Neither log carries a shared correlation ID, so matching is heuristic:
 * candidates are first bucketed by exact size, then filtered by native thread
 * ID (must match when both sides have one), and finally ranked by timestamp
 * proximity within a tolerance window ({@link #MATCH_TOLERANCE_SECONDS}).
 * Each trace entry is consumed at most once to avoid double-counting.
 * <p>
 * The trace log only records allocations above the configured threshold, so
 * small allocations in the RMM log will never match and appear as
 * "unattributed" in the report.
 */
public final class GpuMemoryAnalyzer
{
    private static final double MATCH_TOLERANCE_SECONDS = 0.1;
    private static final long TIMELINE_MIN_SIZE = 100L * 1024 * 1024;
    private static final double TIMELINE_WINDOW_SECONDS = 3.0;

    private static final List<String> SKIP_FRAME_PREFIXES = List.of(
            "io.trino.tests.benchmark.AllocationTraceHandler");

    private GpuMemoryAnalyzer() {}

    public static void analyze(Path rmmLogPath, Path traceLogPath, PrintStream out)
            throws IOException
    {
        out.printf("Reading RMM log: %s%n", rmmLogPath);
        List<RmmEvent> events = parseRmmLog(rmmLogPath);
        out.printf("  %d events%n", events.size());

        out.printf("Reading trace log: %s%n", traceLogPath);
        List<TraceEntry> traces = parseTraceLog(traceLogPath);
        out.printf("  %d traced entries%n", traces.size());

        reportFailures(out, traces);

        PeakSnapshot peak = findPeak(events);
        out.println();
        printSeparator(out);
        out.printf("Peak device memory: %s at %s%n", succinctBytes(peak.peakBytes), peak.peakTime);
        out.printf("Live allocations at peak: %d%n", peak.liveAtPeak.size());
        printSeparator(out);

        TraceIndex traceIndex = buildTraceIndex(traces);
        Map<String, List<String>> allocMatched = matchAllocations(peak.liveAtPeak, traceIndex);
        out.printf("Matched %d/%d allocations to stack traces%n", allocMatched.size(), peak.liveAtPeak.size());

        reportConsumers(out, peak, allocMatched);
        reportTimeline(out, events, peak, traceIndex);
    }

    private static List<RmmEvent> parseRmmLog(Path path)
            throws IOException
    {
        List<RmmEvent> events = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                // Thread,Time,Action,Pointer,Size,Stream
                String[] parts = line.split(",");
                checkState(parts.length == 6, "Unexpected column count for RMM log: %s", parts.length);
                events.add(new RmmEvent(parts[1], parts[0], parts[2], parts[3], Long.parseLong(parts[4])));
            }
        }
        return events;
    }

    private static List<TraceEntry> parseTraceLog(Path path)
            throws IOException
    {
        List<TraceEntry> traces = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("Timestamp")) {
                    continue;
                }
                // Timestamp, Thread, Action, Size, StackFrames
                String[] parts = line.split("\t");
                checkState(parts.length == 5, "Unexpected column count for trace log: %s", parts.length);
                List<String> frames = List.of(parts[4].split("\\|"));
                traces.add(new TraceEntry(parts[0], parts[1], parts[2], Long.parseLong(parts[3]), frames));
            }
        }
        return traces;
    }

    private static PeakSnapshot findPeak(List<RmmEvent> events)
    {
        Map<String, LiveAllocation> live = new HashMap<>();
        long current = 0;
        long peak = 0;
        String peakTime = "";
        Map<String, LiveAllocation> peakSnapshot = Map.of();

        for (RmmEvent event : events) {
            if ("allocate".equals(event.action)) {
                live.put(event.pointer, new LiveAllocation(event.size, event.time, event.thread));
                current += event.size;
                if (current > peak) {
                    peak = current;
                    peakTime = event.time;
                    peakSnapshot = new HashMap<>(live);
                }
            }
            else if ("free".equals(event.action)) {
                LiveAllocation removed = live.remove(event.pointer);
                if (removed != null) {
                    current -= removed.size;
                }
            }
        }

        return new PeakSnapshot(peak, peakTime, peakSnapshot);
    }

    private static TraceIndex buildTraceIndex(List<TraceEntry> traces)
    {
        Map<Long, List<IndexedTrace>> allocatesBySize = new HashMap<>();
        Map<Long, List<IndexedTrace>> deallocatesBySize = new HashMap<>();
        for (int i = 0; i < traces.size(); i++) {
            TraceEntry trace = traces.get(i);
            IndexedTrace indexed = new IndexedTrace(parseTimeToSeconds(trace.time), trace.thread, trace.frames, i);
            Map<Long, List<IndexedTrace>> target = switch (trace.action) {
                case "allocate", "allocate_failed" -> allocatesBySize;
                case "deallocate" -> deallocatesBySize;
                default -> null;
            };
            if (target != null) {
                target.computeIfAbsent(trace.size, _ -> new ArrayList<>()).add(indexed);
            }
        }
        allocatesBySize.values().forEach(list -> list.sort(Comparator.comparingDouble(t -> t.timeSeconds)));
        deallocatesBySize.values().forEach(list -> list.sort(Comparator.comparingDouble(t -> t.timeSeconds)));
        return new TraceIndex(allocatesBySize, deallocatesBySize);
    }

    private static Map<String, List<String>> matchAllocations(Map<String, LiveAllocation> liveAtPeak, TraceIndex traceIndex)
    {
        Map<String, List<String>> matched = new HashMap<>();
        Set<Integer> used = new HashSet<>();

        for (Map.Entry<String, LiveAllocation> entry : liveAtPeak.entrySet()) {
            String pointer = entry.getKey();
            LiveAllocation alloc = entry.getValue();
            findBestMatchingFrames(traceIndex.allocatesBySize, used, alloc.size, parseTimeToSeconds(alloc.time), alloc.thread)
                    .ifPresent(frames -> matched.put(pointer, frames));
        }

        return matched;
    }

    private static Optional<List<String>> findBestMatchingFrames(Map<Long, List<IndexedTrace>> tracesBySize, Set<Integer> used, long size, double timeSeconds, String thread)
    {
        List<IndexedTrace> candidates = tracesBySize.get(size);
        if (candidates == null) {
            return Optional.empty();
        }
        int bestIndex = -1;
        double bestDelta = Double.MAX_VALUE;
        List<String> bestFrames = null;
        for (IndexedTrace candidate : candidates) {
            if (used.contains(candidate.index)) {
                continue;
            }
            if (!thread.equals(candidate.thread)) {
                continue;
            }
            double delta = abs(timeSeconds - candidate.timeSeconds);
            if (delta >= MATCH_TOLERANCE_SECONDS) {
                continue;
            }
            if (delta < bestDelta) {
                bestDelta = delta;
                bestIndex = candidate.index;
                bestFrames = candidate.frames;
            }
        }
        if (bestIndex >= 0) {
            used.add(bestIndex);
        }
        return Optional.ofNullable(bestFrames);
    }

    private static void reportFailures(PrintStream out, List<TraceEntry> traces)
    {
        List<TraceEntry> failures = traces.stream()
                .filter(t -> "allocate_failed".equals(t.action))
                .toList();
        if (failures.isEmpty()) {
            return;
        }
        out.println();
        printSeparator(out);
        out.printf("ALLOCATION FAILURES: %d%n", failures.size());
        printSeparator(out);
        for (TraceEntry failure : failures) {
            out.printf("  %s  %12s  %s%n", failure.time, succinctBytes(failure.size), callPathSignature(failure.frames));
        }
    }

    private static void reportConsumers(PrintStream out, PeakSnapshot peak, Map<String, List<String>> matched)
    {
        Map<String, ConsumerGroup> groups = new LinkedHashMap<>();
        long unattributedTotal = 0;
        int unattributedCount = 0;

        for (Map.Entry<String, LiveAllocation> entry : peak.liveAtPeak.entrySet()) {
            String pointer = entry.getKey();
            LiveAllocation alloc = entry.getValue();
            List<String> frames = matched.get(pointer);
            if (frames != null) {
                String signature = callPathSignature(frames);
                groups.compute(signature, (_, existing) -> {
                    if (existing == null) {
                        List<Long> sizes = new ArrayList<>();
                        sizes.add(alloc.size);
                        return new ConsumerGroup(alloc.size, 1, sizes, frames);
                    }
                    existing.sizes.add(alloc.size);
                    return new ConsumerGroup(existing.totalBytes + alloc.size, existing.count + 1, existing.sizes, existing.exampleFrames);
                });
            }
            else {
                unattributedTotal += alloc.size;
                unattributedCount++;
            }
        }

        out.println();
        printSeparator(out);
        out.println("MEMORY CONSUMERS AT PEAK (grouped by call path)");
        printSeparator(out);

        groups.entrySet().stream()
                .sorted(Map.Entry.<String, ConsumerGroup>comparingByValue(Comparator.comparingLong(g -> g.totalBytes)).reversed())
                .forEach(entry -> {
                    String signature = entry.getKey();
                    ConsumerGroup group = entry.getValue();
                    double percent = peak.peakBytes > 0 ? group.totalBytes * 100.0 / peak.peakBytes : 0;
                    out.printf("%n  %12s (%5.1f%%)  %d allocation(s)%n", succinctBytes(group.totalBytes), percent, group.count);
                    out.printf("  Call path: %s%n", signature);

                    List<Long> sorted = group.sizes.stream().sorted(Comparator.reverseOrder()).toList();
                    if (sorted.size() <= 5) {
                        sorted.forEach(size -> out.printf("    %s%n", succinctBytes(size)));
                    }
                    else {
                        sorted.stream().limit(3).forEach(size -> out.printf("    %s%n", succinctBytes(size)));
                        out.printf("    ... and %d more (smallest: %s)%n", sorted.size() - 3, succinctBytes(sorted.getLast()));
                    }

                    out.println("  Stack trace:");
                    for (String frame : group.exampleFrames) {
                        if (SKIP_FRAME_PREFIXES.stream().anyMatch(frame::startsWith)) {
                            continue;
                        }
                        out.printf("    %s%n", frame);
                        if (frame.startsWith("io.trino.operator.gpu.GpuOperator")) {
                            break;
                        }
                    }
                });

        if (unattributedCount > 0) {
            double percent = peak.peakBytes > 0 ? unattributedTotal * 100.0 / peak.peakBytes : 0;
            out.printf("%n  %12s (%5.1f%%)  %d allocation(s)%n", succinctBytes(unattributedTotal), percent, unattributedCount);
            out.println("  (unattributed — below trace threshold or unmatched)");
        }
    }

    private static void reportTimeline(PrintStream out, List<RmmEvent> events, PeakSnapshot peak, TraceIndex traceIndex)
    {
        out.println();
        printSeparator(out);
        out.println("TIMELINE (large allocations in the 3s before peak)");
        printSeparator(out);

        double peakSeconds = parseTimeToSeconds(peak.peakTime);
        double windowStart = peakSeconds - TIMELINE_WINDOW_SECONDS;

        Set<Integer> allocUsed = new HashSet<>();
        Set<Integer> freeUsed = new HashSet<>();

        for (RmmEvent event : events) {
            double time = parseTimeToSeconds(event.time);
            if (time < windowStart || time > peakSeconds + 0.1) {
                continue;
            }
            if (event.size < TIMELINE_MIN_SIZE) {
                continue;
            }
            String marker = "allocate".equals(event.action) ? "+++" : "---";
            String traceInfo = "";
            if ("allocate".equals(event.action)) {
                Optional<List<String>> frames = findBestMatchingFrames(traceIndex.allocatesBySize, allocUsed, event.size, time, event.thread);
                if (frames.isPresent()) {
                    traceInfo = "  " + callPathSignature(frames.get(), 2);
                }
            }
            else if ("free".equals(event.action)) {
                Optional<List<String>> frames = findBestMatchingFrames(traceIndex.deallocatesBySize, freeUsed, event.size, time, event.thread);
                if (frames.isPresent()) {
                    traceInfo = "  " + callPathSignature(frames.get(), 2);
                }
            }
            out.printf("  %s %s  T%s  %12s%s%n", marker, event.time, event.thread, succinctBytes(event.size), traceInfo);
        }
    }

    private static String callPathSignature(List<String> frames)
    {
        return callPathSignature(frames, 4);
    }

    private static String callPathSignature(List<String> frames, int depth)
    {
        List<String> meaningful = new ArrayList<>();
        for (String frame : frames) {
            if (SKIP_FRAME_PREFIXES.stream().anyMatch(frame::startsWith)) {
                continue;
            }
            int paren = frame.indexOf('(');
            meaningful.add(paren >= 0 ? frame.substring(0, paren) : frame);
            if (meaningful.size() >= depth) {
                break;
            }
        }
        return meaningful.isEmpty() ? "<cudf-internal>" : String.join(" <- ", meaningful);
    }

    private static double parseTimeToSeconds(String time)
    {
        // HH:mm:ss.SSSSSS
        String[] parts = time.split(":");
        int hours = Integer.parseInt(parts[0]);
        int minutes = Integer.parseInt(parts[1]);
        double seconds = Double.parseDouble(parts[2]);
        return hours * 3600.0 + minutes * 60.0 + seconds;
    }

    private static void printSeparator(PrintStream out)
    {
        out.println("=".repeat(70));
    }

    private record RmmEvent(String time, String thread, String action, String pointer, long size)
    {
        public RmmEvent
        {
            requireNonNull(time, "time is null");
            requireNonNull(thread, "thread is null");
            requireNonNull(action, "action is null");
            requireNonNull(pointer, "pointer is null");
        }
    }

    private record TraceEntry(String time, String thread, String action, long size, List<String> frames)
    {
        public TraceEntry
        {
            requireNonNull(time, "time is null");
            requireNonNull(thread, "thread is null");
            requireNonNull(action, "action is null");
            requireNonNull(frames, "frames is null");
        }
    }

    private record LiveAllocation(long size, String time, String thread)
    {
        public LiveAllocation
        {
            requireNonNull(time, "time is null");
            requireNonNull(thread, "thread is null");
        }
    }

    private record PeakSnapshot(long peakBytes, String peakTime, Map<String, LiveAllocation> liveAtPeak)
    {
        public PeakSnapshot
        {
            requireNonNull(peakTime, "peakTime is null");
            requireNonNull(liveAtPeak, "liveAtPeak is null");
        }
    }

    private record IndexedTrace(double timeSeconds, String thread, List<String> frames, int index)
    {
        public IndexedTrace
        {
            requireNonNull(thread, "thread is null");
            requireNonNull(frames, "frames is null");
        }
    }

    private record TraceIndex(
            Map<Long, List<IndexedTrace>> allocatesBySize,
            Map<Long, List<IndexedTrace>> deallocatesBySize)
    {
        public TraceIndex
        {
            requireNonNull(allocatesBySize, "allocatesBySize is null");
            requireNonNull(deallocatesBySize, "deallocatesBySize is null");
        }
    }

    private record ConsumerGroup(long totalBytes, int count, List<Long> sizes, List<String> exampleFrames)
    {
        public ConsumerGroup
        {
            requireNonNull(sizes, "sizes is null");
            requireNonNull(exampleFrames, "exampleFrames is null");
        }
    }
}
