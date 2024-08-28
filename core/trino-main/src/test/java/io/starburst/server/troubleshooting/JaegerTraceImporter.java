/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import io.opentelemetry.exporter.internal.grpc.GrpcExporter;
import io.opentelemetry.exporter.internal.grpc.GrpcExporterBuilder;
import io.opentelemetry.exporter.internal.marshal.Marshaler;
import io.opentelemetry.exporter.internal.marshal.Serializer;
import io.opentelemetry.sdk.common.CompletableResultCode;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

/**
 * This is an application for importing OpenTelemetry traces produced by Run&Troubleshoot to a local Jaeger instance.
 * For more information, see [project root directory]/architecture/performance/troubleshooting.md
 */
public final class JaegerTraceImporter
{
    private JaegerTraceImporter() {}

    public static void main(String[] args)
            throws IOException
    {
        GrpcExporterBuilder grpcExporterBuilder = new GrpcExporterBuilder("otlp", "span", 10L, URI.create("http://localhost:4317"), () -> null, "/opentelemetry.proto.collector.trace.v1.TraceService/Export");
        GrpcExporter exporter = grpcExporterBuilder.build();
        Path tracesDirectory = Paths.get(System.getProperty("user.home")).resolve("Downloads/traces");
        Path decompressedDirectory = tracesDirectory.resolve("decompressed");
        try (Stream<Path> traceFiles = Files.list(tracesDirectory)) {
            for (Path traceFile : traceFiles.toList()) {
                String traceFileName = traceFile.getFileName().toString();
                if (traceFileName.endsWith(".grpc")) {
                    exportTrace(exporter, traceFile);
                }
                else if (traceFileName.endsWith(".grpc.gz")) {
                    Files.createDirectories(decompressedDirectory);
                    Path decompressedTraceFile = decompressedDirectory
                            .resolve(traceFile.getFileName())
                            .resolveSibling(traceFileName.substring(0, traceFileName.lastIndexOf(".gz")));
                    decompressGzip(traceFile, decompressedTraceFile);
                    exportTrace(exporter, decompressedTraceFile);
                }
                else {
                    System.out.println("skipped path: " + traceFile + ", it's not a .grpc or .grpc.gz file");
                }
            }
        }
        exporter.shutdown();
    }

    private static void decompressGzip(Path source, Path target)
    {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(
                new FileInputStream(source.toFile()))) {
            Files.copy(gzipInputStream, target, REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void exportTrace(GrpcExporter exporter, Path file)
    {
        CompletableResultCode resultCode = exporter.export(new FileBasedMarshaller(file), 1);
        CompletableResultCode joinResult = resultCode.join(10, TimeUnit.SECONDS);
        System.out.println("done: " + joinResult.isDone() + ", success: " + joinResult.isSuccess() + ", path: " + file);
    }

    private static class FileBasedMarshaller
            extends Marshaler
    {
        private final Path file;

        public FileBasedMarshaller(Path file)
        {
            this.file = requireNonNull(file, "file is null");
        }

        @Override
        public int getBinarySerializedSize()
        {
            try {
                return toIntExact(Files.size(file));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void writeTo(Serializer serializer)
                throws IOException
        {
            serializer.writeSerializedMessage(Files.readAllBytes(file), null);
        }
    }
}
