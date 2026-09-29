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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.testing.containers.Floci;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.Objects.requireNonNull;

/**
 * Floci-backed S3 storage for SAS connector tests: hosts {@code .sas7bdat} files and produces the
 * catalog properties a {@code sas.data-directory=s3://...} catalog needs to reach them.
 */
public final class SasFlociStorage
        implements Closeable
{
    private final Floci floci;

    private SasFlociStorage(Floci floci)
    {
        this.floci = requireNonNull(floci, "floci is null");
    }

    public static SasFlociStorage start()
    {
        Floci floci = new Floci();
        floci.start();
        return new SasFlociStorage(floci);
    }

    public void createBucket(String bucket)
    {
        floci.createBucket(bucket);
    }

    /**
     * @return the {@code s3://} location of {@code prefix} inside {@code bucket}; an empty prefix
     *         addresses the bucket root
     */
    public static String location(String bucket, String prefix)
    {
        if (prefix.isEmpty()) {
            return "s3://" + bucket;
        }
        return "s3://" + bucket + "/" + prefix;
    }

    public Map<String, String> catalogProperties(String dataDirectory)
    {
        return ImmutableMap.<String, String>builder()
                .put("sas.data-directory", dataDirectory)
                .put("s3.endpoint", floci.endpoint().toString())
                .put("s3.region", FLOCI_REGION)
                .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                .put("s3.path-style-access", "true")
                .buildOrThrow();
    }

    public S3FileSystemFactory createFileSystemFactory()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setEndpoint(floci.endpoint().toString())
                .setRegion(FLOCI_REGION)
                .setAwsAccessKey(FLOCI_ACCESS_KEY)
                .setAwsSecretKey(FLOCI_SECRET_KEY)
                .setPathStyleAccess(true);
        return new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());
    }

    /**
     * Uploads every regular file under {@code localDirectory} to {@code bucket}, keeping the
     * directory layout below {@code targetPrefix}.
     */
    public void uploadDirectory(Path localDirectory, String bucket, String targetPrefix)
            throws IOException
    {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(localDirectory)) {
            files = walk.filter(Files::isRegularFile).collect(toImmutableList());
        }
        for (Path file : files) {
            String relativePath = localDirectory.relativize(file).toString();
            floci.putObject(bucket, Files.readAllBytes(file), key(targetPrefix, relativePath));
        }
    }

    public void putObject(String bucket, String key, byte[] contents)
    {
        floci.putObject(bucket, contents, key);
    }

    private static String key(String prefix, String relativePath)
    {
        if (prefix.isEmpty()) {
            return relativePath;
        }
        return prefix + "/" + relativePath;
    }

    @Override
    public void close()
    {
        floci.close();
    }
}
