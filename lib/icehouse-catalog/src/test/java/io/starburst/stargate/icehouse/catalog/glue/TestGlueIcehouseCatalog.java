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
package io.starburst.stargate.icehouse.catalog.glue;

import io.starburst.stargate.icehouse.exception.CatalogAccessDeniedException;
import io.starburst.stargate.icehouse.exception.RetryableIcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.InternalServiceException;

import static org.assertj.core.api.Assertions.assertThat;

class TestGlueIcehouseCatalog
{
    @Test
    void testAccessDeniedIsClassifiedAsCatalogAccessDenied()
    {
        AccessDeniedException accessDenied = AccessDeniedException.builder()
                .message("User: arn:aws:iam::123:role/x is not authorized to perform: glue:GetTables")
                .build();

        RuntimeException classified = GlueIcehouseCatalog.classifyGlueException("metadataLocation(schema.table)", accessDenied);

        assertThat(classified).isInstanceOf(CatalogAccessDeniedException.class);
        assertThat(classified.getCause()).isSameAs(accessDenied);
    }

    @Test
    void testEntityNotFoundIsClassifiedAsTerminalButNotAccessDenied()
    {
        EntityNotFoundException notFound = EntityNotFoundException.builder()
                .message("Table not found")
                .build();

        RuntimeException classified = GlueIcehouseCatalog.classifyGlueException("listTables(schema)", notFound);

        assertThat(classified).isInstanceOf(TerminalIcehouseCatalogException.class);
        assertThat(classified).isNotInstanceOf(CatalogAccessDeniedException.class);
    }

    @Test
    void testUnknownGlueExceptionIsClassifiedAsRetryable()
    {
        InternalServiceException internalError = InternalServiceException.builder()
                .message("Internal error")
                .build();

        RuntimeException classified = GlueIcehouseCatalog.classifyGlueException("listSchemas", internalError);

        assertThat(classified).isInstanceOf(RetryableIcehouseCatalogException.class);
    }
}
