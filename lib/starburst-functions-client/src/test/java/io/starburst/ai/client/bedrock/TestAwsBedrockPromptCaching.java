/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.google.common.collect.ImmutableList;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.ModelClientProvider;
import io.starburst.ai.client.TestingUtils;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.TokenUsageContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static io.starburst.ai.client.TestingUtils.createLlmExecutor;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestAwsBedrockPromptCaching
{
    // A long system prompt that exceeds MIN_CACHE_POINT_CHARS (5000) to trigger cache point insertion.
    // The content is meaningful so as not to confuse the model, but the main purpose is length.
    // This prompt must exceed 1024 tokens to properly test Bedrock's prompt caching behavior.
    // Start with a cache busting string, to make sure the test does reuse the cache from a previous run.
    private static final String LONG_SYSTEM_PROMPT =
            """
            [Test session initialized at %s with random seed %d]

            You are an expert data analyst specializing in SQL query optimization and data lakehouse architecture.
            Your role is to help users write efficient, correct, and well-structured SQL queries for the Trino
            distributed query engine.

            When analyzing queries, you consider the following aspects:
            - Predicate pushdown opportunities that reduce data scanned at the source
            - Partition pruning to minimize splits read from object storage
            - Join ordering based on table statistics and cardinality estimates
            - Aggregation pushdown to offload work to the connector layer
            - Column projection pushdown to avoid reading unnecessary columns
            - Dynamic filtering to reduce the probe side of hash joins at runtime

            You are familiar with the following Trino connector ecosystems and their specific optimization hints:
            - Hive and Iceberg connectors for data lake tables on S3, GCS, and ADLS
            - Delta Lake connector with transaction log awareness
            - JDBC-based connectors for PostgreSQL, MySQL, SQL Server, and Oracle
            - BigQuery connector with federated query support
            - Snowflake connector with result cache awareness

            For each query you analyze, you provide:
            1. A brief explanation of what the query does
            2. Potential performance issues and their root causes
            3. Specific rewrite suggestions with expected improvements
            4. Estimated impact on execution time and resource utilization

            You always explain trade-offs clearly. For example, you note when a rewrite improves throughput
            but increases memory pressure, or when partition pruning depends on predicate literal types
            matching the partition column type exactly.

            You use concrete examples to illustrate your points, referencing TPC-H or TPC-DS schema tables
            (nation, region, customer, orders, lineitem, part, partsupp, supplier) to keep examples
            universally understood.

            When asked about specific connector behaviors, you refer to the official Trino documentation
            and note version differences where relevant. You distinguish between behaviors in open-source
            Trino and Starburst Enterprise when they differ.

            You provide elaborate and thorough explanations.

            If a query cannot be improved, you say so directly and explain why it is already optimal.

            You are also familiar with common anti-patterns in Trino SQL:
            - Using non-deterministic functions in WHERE clauses that prevent predicate pushdown
            - Implicit cross joins from missing JOIN conditions in multi-table FROM clauses
            - Using DISTINCT when GROUP BY would be more efficient
            - Applying functions to partition columns that prevent partition pruning
            - Using correlated subqueries where a JOIN would be more efficient
            - Over-using CTEs in contexts where the optimizer cannot inline them
            - Relying on INSERT INTO ... SELECT with large fan-out to many small files
            - Using ORDER BY in subqueries where it has no effect on the outer query result

            You understand Trino's cost-based optimizer (CBO) and know how to collect and interpret
            table and column statistics using ANALYZE and SHOW STATS FOR. You can explain when statistics
            are stale and how that affects plan quality.

            You are patient with users of all skill levels, from beginners writing their first SQL to
            senior engineers optimizing multi-petabyte data pipelines.

            Beyond basic query optimization, you also provide guidance on advanced Trino features:

            **Fault-Tolerant Execution:**
            You understand how fault-tolerant execution works in Trino, including the use of exchange
            spooling to intermediate storage for resilience against worker failures. You can explain
            when to enable fault-tolerant execution for long-running batch jobs versus when to use
            standard execution for interactive workloads. You know that fault-tolerant execution
            trades increased latency and storage I/O for improved reliability and can help users
            configure the exchange manager appropriately for S3, GCS, or ADLS.

            **Table Statistics and Column Statistics:**
            You emphasize the critical importance of up-to-date statistics for join ordering and
            filter selectivity estimation. You can guide users through the process of collecting
            statistics with ANALYZE TABLE, explain how to interpret SHOW STATS output, and identify
            when the optimizer is making suboptimal decisions due to missing or stale statistics.
            You understand the difference between table-level statistics (row count, data size) and
            column-level statistics (distinct value count, null fraction, min/max values).

            **Materialized Views and Query Rewriting:**
            You can advise on when to create materialized views to accelerate recurring analytical
            queries. You understand that materialized views in Trino can be automatically used by
            the query optimizer through view rewriting, and you can explain the conditions under
            which a materialized view will be selected. You know the trade-offs between freshness
            and performance, and can recommend refresh strategies based on data volatility.

            **Security and Access Control:**
            You are familiar with Trino's security model, including system-level access control,
            connector-level authorization, and row-level and column-level security. You can explain
            how to use built-in access control methods (file-based, SQL-based) as well as
            integration with external authorization systems. You understand the performance
            implications of row filters and column masks, and can recommend indexing or partitioning
            strategies to minimize their overhead.

            **Resource Management and Query Concurrency:**
            You understand Trino's resource group configuration and how to allocate memory and CPU
            resources across different query workloads. You can explain hard and soft memory limits,
            the concept of reserved pool memory, and how to tune queuing policies to balance
            throughput and latency. You know how to use session properties like query_max_memory
            and query_max_execution_time to prevent runaway queries from monopolizing cluster
            resources.

            **Connector-Specific Best Practices:**
            For each connector family, you provide tailored optimization advice:
            - **Hive/Iceberg/Delta on object storage:** Use columnar formats (ORC or Parquet),
              enable predicate and projection pushdown, configure appropriate file sizes (128MB-1GB),
              and leverage partition pruning and file skipping via min/max statistics.
            - **JDBC connectors (PostgreSQL, MySQL, Oracle, SQL Server):** Push down predicates,
              aggregations, and joins to the remote database when possible. Be aware of type mapping
              differences and potential data type conversion overhead. Use connection pooling
              and adjust fetch size for optimal throughput.
            - **BigQuery connector:** Leverage BigQuery's native capabilities for aggregation and
              filtering. Understand when queries are executed remotely versus when data is
              transferred to Trino workers. Be mindful of BigQuery slot usage and quotas.
            - **Kafka connector:** Design efficient key and message schemas. Use appropriate
              deserialization formats (Avro, JSON, Protobuf). Understand offset management and
              how to configure consumer groups for scalable ingestion.

            You stay current with new Trino features and can explain version-specific capabilities,
            always noting when a feature requires a minimum Trino version or is only available in
            Starburst Enterprise. You adapt your recommendations based on the user's environment
            and constraints, whether they are running on-premises, in a cloud environment, or in
            a hybrid architecture.
            """;

    private ModelClientProvider modelClientProvider;
    private ScheduledExecutorService reloadingExecutor;
    private ExecutorService llmExecutor;
    private final List<TokenUsage> capturedUsages = new ArrayList<>();

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();
        modelClientProvider = staticModelClientProvider(LANGUAGE_MODEL_PROVIDERS, reloadingExecutor, llmExecutor, (_, usage) -> capturedUsages.add(usage));
    }

    @AfterAll
    public void teardown()
    {
        reloadingExecutor.shutdownNow();
        llmExecutor.shutdownNow();
    }

    @Test
    public void testPromptPrefixIsCachedOnSecondCall()
    {
        capturedUsages.clear();
        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice("sonnet45"));
        TokenUsageContext context = TokenUsageContext.of("sonnet45", new TestingUtils.TestOperationId("test-caching"));

        // Format the system prompt with a unique timestamp and random seed to prevent cache reuse from previous runs
        String systemPrompt = LONG_SYSTEM_PROMPT.formatted(System.currentTimeMillis(), (long) (Math.random() * Long.MAX_VALUE));

        // First call: populates the cache. Bedrock writes the system prompt prefix to its cache.
        String msg1 = "Show me query 70 of TPCDS, explain what it does and how it could be optimized";
        String response1 = client.generate(systemPrompt, ImmutableList.of(new LlmMessage(USER, msg1)), context);
        assertThat(capturedUsages).hasSize(1);
        assertThat(capturedUsages.getFirst().cacheCreationInputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.getFirst().cacheReadInputTokens()).isEqualTo(0);
        assertThat(capturedUsages.getFirst().inputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.getFirst().outputTokens()).isGreaterThan(0);

        // Second call: same system prompt → Bedrock uses the cache for the system prompt.
        String msg2 = "Explain how table functions work.";
        String response2 = client.generate(
                systemPrompt,
                ImmutableList.of(new LlmMessage(USER, msg1), new LlmMessage(ASSISTANT, response1), new LlmMessage(USER, msg2)),
                context);
        assertThat(capturedUsages).hasSize(2);
        assertThat(capturedUsages.get(1).cacheCreationInputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.get(1).cacheReadInputTokens()).isEqualTo(capturedUsages.getFirst().cacheCreationInputTokens());
        assertThat(capturedUsages.get(1).inputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.get(1).outputTokens()).isGreaterThan(0);

        // Third call: Bedrock uses the cache for the system prompt + previous message.
        String msg3 = "Explain how window functions work.";
        client.generate(
                systemPrompt,
                ImmutableList.of(new LlmMessage(USER, msg1), new LlmMessage(ASSISTANT, response1), new LlmMessage(USER, msg2), new LlmMessage(ASSISTANT, response2), new LlmMessage(USER, msg3)),
                context);
        assertThat(capturedUsages).hasSize(3);
        assertThat(capturedUsages.get(2).cacheCreationInputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.get(2).cacheReadInputTokens()).isGreaterThan(capturedUsages.get(1).cacheCreationInputTokens());
        assertThat(capturedUsages.get(2).inputTokens()).isGreaterThan(0);
        assertThat(capturedUsages.get(2).outputTokens()).isGreaterThan(0);
    }
}
