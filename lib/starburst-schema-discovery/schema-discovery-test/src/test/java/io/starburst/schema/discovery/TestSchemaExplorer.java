/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.SchemaExplorer.DiscoveryConfig;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredIdentifier;
import io.starburst.schema.discovery.options.CommaDelimitedOptionsParser;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static io.starburst.schema.discovery.Util.orcDataSourceFactory;
import static io.starburst.schema.discovery.Util.parquetDataSourceFactory;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static io.trino.spi.StandardErrorCode.PROCEDURE_CALL_FAILED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSchemaExplorer
{
    @Test
    public void testDiscoverRethrowsExistingTrinoException()
    {
        ErroringTrinoFileSystem erroringFileSystem = new ErroringTrinoFileSystem("limits/t1", "limits/t1", 4, true);
        SchemaDiscoveryController controller = new SchemaDiscoveryController(
                _ -> new DiscoveryTrinoFileSystem(erroringFileSystem), parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        SchemaExplorer schemaExplorer = new SchemaExplorer(controller, new ObjectMapperProvider().get(), new CommaDelimitedOptionsParser(ImmutableList.of(GeneralOptions.class)));
        DiscoveryConfig discoveryConfig = new DiscoveryConfig(
                uriFromLocation(erroringFileSystem.directory()).toString(),
                "",
                new GenerateOptions(DiscoveredIdentifier.of("discovered"), 10, true, Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // the underlying failure ("listDirectories fail") is already surfaced as a TrinoException(IO, ...) by
        // DiscoveryTrinoFileSystem; SchemaExplorer must rethrow it as-is rather than wrapping it in a new
        // TrinoException(PROCEDURE_CALL_FAILED, ...), which would mask the original error code
        assertThatThrownBy(() -> schemaExplorer.discover(discoveryConfig, _ -> {}))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Could not list directories in")
                .matches(e -> ((TrinoException) e).getErrorCode().getName().equals(IO.name()))
                .matches(e -> !((TrinoException) e).getErrorCode().getName().equals(PROCEDURE_CALL_FAILED.name()))
                .rootCause().hasMessage("listDirectories fail");
    }
}
