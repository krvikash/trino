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
package io.trino.plugin.warp;

import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.warp.WarpSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.warp.WarpSessionProperties.UNSUPPORTED_FUNCTIONS;
import static io.trino.plugin.warp.WarpSessionProperties.UNSUPPORTED_NATIVE_FUNCTIONS;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WarpSessionPropertiesTest
{
    @Test
    public void testNoNullDefaultValues()
    {
        List<String> allowedNullProps = List.of(
                WarpSessionProperties.IMPORT_EXPORT_S3_PATH,
                WarpSessionProperties.UNSUPPORTED_FUNCTIONS,
                WarpSessionProperties.UNSUPPORTED_NATIVE_FUNCTIONS,
                WarpSessionProperties.ENABLE_DICTIONARY);
        WarpSessionProperties warpSessionProperties = new WarpSessionProperties(new SharedConfig(), new GlobalConfig());
        List<PropertyMetadata<?>> sessionProperties = warpSessionProperties.getSessionProperties();
        sessionProperties.stream().filter(sessionProperty -> !allowedNullProps.contains(sessionProperty.getName())).forEach(property -> assertThat(property.getDefaultValue()).isNotNull());
    }

    @Test
    public void testS3ImportExportPath()
    {
        String s3StorePath = WarpSessionProperties.getS3ImportExportPath("s3://dev-andromeda-ws-backup-552728471257-uswt2/resiliency");
        assertThat(s3StorePath).isEqualTo("s3://dev-andromeda-ws-backup-552728471257-uswt2/resiliency/import_export");
    }

    @Test
    public void testUnsupportedFunctions()
    {
        GlobalConfig globalConfig = new GlobalConfig();
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        Set<String> unsupportedFunctions = WarpSessionProperties.getUnsupportedFunctions(connectorSession, globalConfig);
        assertThat(unsupportedFunctions).isEqualTo(Set.of());

        globalConfig.setUnsupportedFunctions("  is_nan   , ceil  ");
        unsupportedFunctions = WarpSessionProperties.getUnsupportedFunctions(connectorSession, globalConfig);
        assertThat(unsupportedFunctions).isEqualTo(Set.of("is_nan", "ceil"));

        when(connectorSession.getProperty(UNSUPPORTED_FUNCTIONS, String.class)).thenReturn("");
        unsupportedFunctions = WarpSessionProperties.getUnsupportedFunctions(connectorSession, globalConfig);
        assertThat(unsupportedFunctions).isEqualTo(Set.of());

        when(connectorSession.getProperty(UNSUPPORTED_FUNCTIONS, String.class)).thenReturn("ceil, $like");
        unsupportedFunctions = WarpSessionProperties.getUnsupportedFunctions(connectorSession, globalConfig);
        assertThat(unsupportedFunctions).isEqualTo(Set.of("ceil", LIKE_FUNCTION_NAME.getName()));
    }

    @Test
    public void testUnsupportedNativeFunctions()
    {
        NativeConfig nativeConfig = new NativeConfig();
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        Set<String> unsupportedNativeFunctions = WarpSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfig);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of());

        nativeConfig.setUnsupportedNativeFunctions("  is_nan   ,  ceil   ");
        unsupportedNativeFunctions = WarpSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfig);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of("is_nan", "ceil"));

        when(connectorSession.getProperty(UNSUPPORTED_NATIVE_FUNCTIONS, String.class)).thenReturn("");
        unsupportedNativeFunctions = WarpSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfig);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of());

        when(connectorSession.getProperty(UNSUPPORTED_NATIVE_FUNCTIONS, String.class)).thenReturn("  ceil ,  $greater_than   ");
        unsupportedNativeFunctions = WarpSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfig);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of("ceil", "$greater_than"));
    }

    @Test
    public void testGetPredicateSimplifyThreshold()
    {
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        GlobalConfig globalConfig = new GlobalConfig();

        assertThat(WarpSessionProperties.getPredicateSimplifyThreshold(connectorSession, globalConfig))
                .isEqualTo(globalConfig.getPredicateSimplifyThreshold());

        when(connectorSession.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(1);

        assertThat(WarpSessionProperties.getPredicateSimplifyThreshold(connectorSession, globalConfig))
                .isEqualTo(1);
    }
}
