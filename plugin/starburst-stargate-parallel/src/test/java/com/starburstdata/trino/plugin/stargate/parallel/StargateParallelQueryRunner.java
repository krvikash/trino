/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.stargate.TestingMemoryPlugin;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spooling.filesystem.FileSystemSpoolingPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class StargateParallelQueryRunner
{
    static final String MEMORY_TPCH_SCHEMA = "tiny";

    private StargateParallelQueryRunner() {}

    public static DistributedQueryRunner createRemoteStarburstQueryRunner(LocalStackContainer localstack, Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        String bucketName = "test-stargate" + UUID.randomUUID();
        try (S3Client client = createS3Client(localstack)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }

        DistributedQueryRunner queryRunner = null;
        try {
            Session session = testSessionBuilder()
                    // Require explicit table qualification or custom session.
                    .setCatalog("unspecified_catalog")
                    .setSchema("unspecified_schema")
                    .build();
            DistributedQueryRunner.Builder<?> queryRunnerBuilder = DistributedQueryRunner.builder(session)
                    .addExtraProperty("protocol.spooling.enabled", "true")
                    .addExtraProperty("protocol.spooling.retrieval-mode", "storage")
                    .addExtraProperty("protocol.spooling.shared-secret-key", random256BitsEncryptionKey())
                    .setWorkerCount(3);

            systemAccessControl.ifPresent(queryRunnerBuilder::setSystemAccessControl);

            queryRunner = queryRunnerBuilder.build();
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new FileSystemSpoolingPlugin());
            queryRunner.loadSpoolingManager("filesystem", Map.of(
                    "fs.s3.enabled", "true",
                    "fs.location", "s3://" + bucketName + "/",
                    "fs.segment.encryption", "false", // Encryption doesn't work with the direct-access yet
                    "fs.segment.pruning.enabled", "true", // We don't need pruning in tests
                    "s3.endpoint", localstack.withServices("s3").getEndpoint().toString(),
                    "s3.region", localstack.getRegion(),
                    "s3.aws-access-key", localstack.getAccessKey(),
                    "s3.aws-secret-key", localstack.getSecretKey()));

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addMemoryToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new TestingMemoryPlugin());
            queryRunner.createCatalog("memory", "testing_memory");

            queryRunner.execute("CREATE SCHEMA memory.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema(MEMORY_TPCH_SCHEMA)
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInMemoryConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addHiveToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            Path hiveCatalog,
            Iterable<TpchTable<?>> requiredTablesInHiveConnector)
            throws Exception
    {
        try {
            queryRunner.installPlugin(new HivePlugin());
            queryRunner.createCatalog("hive", "hive", ImmutableMap.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", "file:" + hiveCatalog.toRealPath(),
                    "hive.security", "allow-all",
                    "fs.hadoop.enabled", "true"));

            queryRunner.execute("CREATE SCHEMA hive.tiny");
            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("hive")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInHiveConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static void addPostgreSqlToRemoteStarburstQueryRunner(
            DistributedQueryRunner queryRunner,
            TestingPostgreSqlServer server,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> requiredTablesInPostgreSqlConnector)
            throws Exception
    {
        try {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            connectorProperties.putIfAbsent("postgresql.include-system-tables", "true");

            server.execute("CREATE SCHEMA tiny");

            queryRunner.installPlugin(new GeoPlugin());
            queryRunner.installPlugin(new PostgreSqlPlugin());
            queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

            Session tpchSetupSession = testSessionBuilder()
                    .setCatalog("postgresql")
                    .setSchema("tiny")
                    .build();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, tpchSetupSession, requiredTablesInPostgreSqlConnector);
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    public static DistributedQueryRunner createRemoteStarburstQueryRunnerWithMemory(
            Iterable<TpchTable<?>> requiredTablesInMemoryConnector,
            LocalStackContainer localstack,
            Optional<SystemAccessControl> systemAccessControl)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createRemoteStarburstQueryRunner(localstack, systemAccessControl);
        addMemoryToRemoteStarburstQueryRunner(queryRunner, requiredTablesInMemoryConnector);
        return queryRunner;
    }

    private static DistributedQueryRunner createStargateQueryRunner(
            boolean enableWrites,
            Map<String, String> extraProperties,
            String catalogName,
            Map<String, String> connectorProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(session);
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder
                    .setCoordinatorProperties(coordinatorProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-user", "p2p");

            queryRunner.installPlugin(new TestingStargateParallelPlugin(enableWrites));
            queryRunner.createCatalog(catalogName, "stargate_parallel", connectorProperties);
            queryRunner.createCatalog(catalogName + "_copy", "stargate_parallel", connectorProperties);

            return queryRunner;
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }
    }

    private static String random256BitsEncryptionKey()
    {
        return Base64.getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());
    }

    private static String stargateConnectionUrl(DistributedQueryRunner stargateQueryRunner, String catalog)
    {
        return connectionUrl(stargateQueryRunner.getCoordinator().getBaseUrl(), catalog);
    }

    private static String connectionUrl(URI trinoUri, String catalog)
    {
        verify(Objects.equals(trinoUri.getScheme(), "http"), "Unsupported scheme: %s", trinoUri.getScheme());
        verify(trinoUri.getUserInfo() == null, "Unsupported user info: %s", trinoUri.getUserInfo());
        verify(Objects.equals(trinoUri.getPath(), ""), "Unsupported path: %s", trinoUri.getPath());
        verify(trinoUri.getQuery() == null, "Unsupported query: %s", trinoUri.getQuery());
        verify(trinoUri.getFragment() == null, "Unsupported fragment: %s", trinoUri.getFragment());

        return format("jdbc:trino://%s/%s", trinoUri.getAuthority(), catalog);
    }

    private static S3Client createS3Client(LocalStackContainer localstack)
    {
        return S3Client.builder()
                .endpointOverride(localstack.withServices("s3").getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
    }

    static void main()
            throws Exception
    {
        Logging.initialize();

        String bucketName = "test-stargate" + UUID.randomUUID();

        LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14.0"));
        localstack.start();

        try (S3Client client = createS3Client(localstack)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }

        DistributedQueryRunner stargateQueryRunner = createRemoteStarburstQueryRunner(localstack, Optional.empty());

        addMemoryToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                TpchTable.getTables());

        TestingPostgreSqlServer postgreSqlServer = new TestingPostgreSqlServer();
        addPostgreSqlToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                postgreSqlServer,
                Map.of("connection-url", postgreSqlServer.getJdbcUrl()),
                TpchTable.getTables());

        Path tempDir = createTempDirectory("HiveCatalog");
        addHiveToRemoteStarburstQueryRunner(
                stargateQueryRunner,
                tempDir,
                TpchTable.getTables());
        DistributedQueryRunner queryRunner = builder(stargateQueryRunner, "memory")
                .enableWrites()
                .withCoordinatorProperties(Map.of("http-server.http.port", "8080"))
                .build();
        queryRunner.createCatalog(
                "p2p_remote_postgresql_parallel",
                "stargate_parallel",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", stargateConnectionUrl(stargateQueryRunner, "postgresql")));
        queryRunner.createCatalog(
                "p2p_remote_hive",
                "stargate_parallel",
                Map.of(
                        "connection-user", "p2p",
                        "connection-url", stargateConnectionUrl(stargateQueryRunner, "hive")));

        Logger log = Logger.get(StargateParallelQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\nRemote Starburst: %s\n====", stargateQueryRunner.getCoordinator().getBaseUrl());
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static Builder builder(DistributedQueryRunner remoteStarburst, String catalog)
    {
        return new Builder(remoteStarburst, catalog);
    }

    public static class Builder
    {
        private boolean enableWrites;
        private String catalogName = "p2p_remote";
        private Map<String, String> connectorProperties = emptyMap();
        private Map<String, String> coordinatorProperties = emptyMap();
        private Map<String, String> extraProperties = emptyMap();

        private Builder(DistributedQueryRunner remoteStarburst, String catalog)
        {
            withConnectorProperties(ImmutableMap.of("connection-url", stargateConnectionUrl(remoteStarburst, catalog)));
        }

        public Builder enableWrites()
        {
            enableWrites = true;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder withCatalog(String catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            return this;
        }

        public Builder withEncoding(String encoding)
        {
            return withConnectorProperties(ImmutableMap.of("encoding", encoding));
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = updateProperties(this.connectorProperties, connectorProperties);
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = updateProperties(this.coordinatorProperties, coordinatorProperties);
            return this;
        }

        public Builder withExtraProperties(Map<String, String> coordinatorProperties)
        {
            this.extraProperties = updateProperties(this.extraProperties, coordinatorProperties);
            return this;
        }

        @SuppressWarnings("unused") // Used in starburst-enterprise repository
        public Builder withStarburstStorage(JdbcDatabaseContainer<?> starburstStorage)
        {
            return withCoordinatorProperties(ImmutableMap.of(
                    "insights.jdbc.url", starburstStorage.getJdbcUrl(),
                    "insights.jdbc.user", starburstStorage.getUsername(),
                    "insights.jdbc.password", starburstStorage.getPassword()));
        }

        @SuppressWarnings("unused") // Used in starburst-enterprise repository
        public Builder withManagedStatistics()
        {
            return withCoordinatorProperties(ImmutableMap.of("starburst.managed-statistics.enabled", "true"))
                    .withConnectorProperties(ImmutableMap.of(
                            "statistics.enabled", "false", // disable collecting native jdbc stats
                            "internal-communication.shared-secret", "internal-shared-secret", // This is required for the internal communication in the managed statistics
                            "managed-statistics.enabled", "true"));
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createStargateQueryRunner(enableWrites, extraProperties, catalogName, connectorProperties, coordinatorProperties);
        }

        private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .buildOrThrow();
        }
    }
}
