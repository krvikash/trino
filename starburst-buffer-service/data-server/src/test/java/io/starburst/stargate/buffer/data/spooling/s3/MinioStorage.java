/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class MinioStorage
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    private final String bucketName;
    private final Network network;
    private final Minio minio;

    public MinioStorage(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.network = newNetwork();
        this.minio = Minio.builder()
                .withNetwork(network)
                .withEnvVars(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", SECRET_KEY)
                        .buildOrThrow())
                .build();
    }

    public void start()
    {
        minio.start();
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:" + minio.getMinioApiEndpoint().getPort()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(US_EAST_1)
                .build();
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3Client.createBucket(createBucketRequest);
    }

    public Minio getMinio()
    {
        return minio;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    @Override
    public void close()
            throws Exception
    {
        network.close();
        minio.close();
    }
}
