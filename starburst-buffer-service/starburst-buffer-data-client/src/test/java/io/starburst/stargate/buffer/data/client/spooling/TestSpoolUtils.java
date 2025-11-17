/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.getS3UriInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSpoolUtils
{
    @Test
    void testGetS3UriInfoWithNormalHost()
    {
        // URI with normal host (no underscore)
        URI uri = URI.create("s3://my-bucket/path/to/file");
        assertThat(getS3UriInfo(uri).bucket()).isEqualTo("my-bucket");
    }

    @Test
    void testGetS3UriInfoWithUnderscore()
    {
        // URI with underscore in bucket name - host is null, uses authority
        // This simulates S3 buckets with underscores (us-east-1)
        URI uri = URI.create("s3://my_bucket/path/to/file");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my_bucket");
        assertThat(s3UriInfo.path()).isEqualTo("path/to/file");
    }

    @Test
    void testGetBucketNameWithUserInfoAndNormalBucket()
    {
        // URI with user info and normal host (no underscore) should return the host
        URI uri = URI.create("s3://user:password@my-bucket/path/to/dir");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my-bucket");
        assertThat(s3UriInfo.path()).isEqualTo("path/to/dir");
    }

    @Test
    void testGetBucketNameWithUserInfoAndUnderscoreBucket()
    {
        // URI with user info and underscore bucket - host is null, userInfo is also null
        // Falls back to authority which includes user info prefix
        URI uri = URI.create("s3://user:password@my_bucket/path/to/dir");
        assertThatThrownBy(() -> getS3UriInfo(uri)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetS3UriInfoWithHostAndPort()
    {
        // URI with host and port
        URI uri = URI.create("s3://my-bucket:9000/path/to/file");
        assertThatThrownBy(() -> getS3UriInfo(uri)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetS3UriInfoWithComplexPath()
    {
        // URI with complex path
        URI uri = URI.create("s3://bucket-name/some/deep/path");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("bucket-name");
        assertThat(s3UriInfo.path()).isEqualTo("/some/deep/path");
    }

    @Test
    void testGetS3UriInfoWithNoPath()
    {
        // URI with just bucket, no path
        URI uri = URI.create("s3://my-bucket");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my-bucket");
        assertThat(s3UriInfo.path()).isEqualTo("");
    }

    @Test
    void testGetS3UriInfoWithMultipleUnderscores()
    {
        // URI with multiple underscores in bucket name
        URI uri = URI.create("s3://my_test_bucket/path");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my_test_bucket");
        assertThat(s3UriInfo.path()).isEqualTo("path");
    }

    @Test
    void testGetS3UriInfoWithDashAndUnderscore()
    {
        // URI with both dash and underscore in bucket name
        URI uri = URI.create("s3://my-test_bucket/path");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my-test_bucket");
        assertThat(s3UriInfo.path()).isEqualTo("path");
    }

    @Test
    void testGetS3UriInfoLeadingAndTrailingSlashes()
    {
        // URI with just bucket, no path
        URI uri = URI.create("s3://my-bucket///some//path///");
        SpoolUtils.S3UriInfo s3UriInfo = getS3UriInfo(uri);
        assertThat(s3UriInfo.bucket()).isEqualTo("my-bucket");
        assertThat(s3UriInfo.path()).isEqualTo("some//path");
    }
}
