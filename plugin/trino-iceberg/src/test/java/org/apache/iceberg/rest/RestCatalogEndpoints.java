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
package org.apache.iceberg.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.iceberg.catalog.rest.RawResponse;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterViewRequest;
import org.apache.iceberg.rest.requests.ImmutableReportMetricsRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.view.ViewVersion;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public final class RestCatalogEndpoints
{
    private RestCatalogEndpoints() {}

    private static final Schema EMPTY_SCHEMA = new Schema(ImmutableList.of());
    private static final Pattern CREDENTIALS_PATTERN = Pattern.compile("v1/namespaces/.+/tables/.+/credentials");
    private static final Pattern REMOTE_SIGN_PATTERN = Pattern.compile("v1/namespaces/.+/tables/.+/sign");

    public static Set<Endpoint> getIcebergEndpoints()
    {
        ImmutableSet.Builder<Endpoint> endpoints = ImmutableSet.builder();
        for (Field field : Endpoint.class.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && Modifier.isFinal(field.getModifiers())
                    && field.getType() == Endpoint.class) {
                try {
                    endpoints.add((Endpoint) field.get(null));
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return endpoints.build();
    }

    public static void assertEndpointValid(String prefix, HTTPRequest.HTTPMethod method, String path, Optional<RESTRequest> body, Optional<Class<?>> responseType)
    {
        String pathWithoutPrefix = path;
        if (!prefix.isEmpty()) {
            int prefixStart = path.indexOf(prefix);
            checkState(prefixStart >= 1, "Invalid prefix: %s", prefix);
            pathWithoutPrefix = path.substring(0, prefixStart) + path.substring(prefixStart + prefix.length() + 1);
        }
        Pair<Route, Map<String, String>> route = Route.from(method, pathWithoutPrefix);
        if (route == null) {
            assertEndpointNotInRoute(method, path, pathWithoutPrefix);
            return;
        }
        if (route.first().responseClass() != null) {
            assertThat(responseType)
                    .describedAs("Expected response type for %s %s", method, path)
                    .isPresent();
            if (responseType.get() != RawResponse.class) {
                assertThat(responseType.get())
                        .describedAs("Response type mismatch for %s %s", method, path)
                        .isEqualTo(route.first().responseClass());
            }
        }
        if (route.first().requestClass() != null) {
            assertThat(body)
                    .describedAs("Expected request body for %s %s", method, path)
                    .isPresent();
            assertThat(body.get())
                    .describedAs("Request body type mismatch for %s %s", method, path)
                    .isInstanceOf(route.first().requestClass());
        }
    }

    private static void assertEndpointNotInRoute(HTTPRequest.HTTPMethod method, String path, String pathWithoutPrefix)
    {
        if (CREDENTIALS_PATTERN.matcher(pathWithoutPrefix).matches()) {
            assertThat(method)
                    .describedAs("Credentials endpoint must be GET: %s", path)
                    .isEqualTo(HTTPRequest.HTTPMethod.GET);
            return;
        }
        if (REMOTE_SIGN_PATTERN.matcher(pathWithoutPrefix).matches()) {
            assertThat(method)
                    .describedAs("Remote sign endpoint must be POST: %s", path)
                    .isEqualTo(HTTPRequest.HTTPMethod.POST);
            return;
        }
        fail("No route for %s %s", method, path);
    }

    public static CreateTableRequest createTableRequest(String name)
    {
        return CreateTableRequest.builder()
                .withName(name)
                .withSchema(EMPTY_SCHEMA)
                .build();
    }

    public static RegisterTableRequest registerTableRequest(String name)
    {
        return ImmutableRegisterTableRequest.builder()
                .name(name)
                .metadataLocation("file:/metadata.json")
                .build();
    }

    public static CreateViewRequest createViewRequest(String name)
    {
        return new CreateViewRequest()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public String location()
            {
                return null;
            }

            @Override
            public Schema schema()
            {
                return EMPTY_SCHEMA;
            }

            @Override
            public ViewVersion viewVersion()
            {
                return null;
            }

            @Override
            public Map<String, String> properties()
            {
                return ImmutableMap.of();
            }
        };
    }

    public static RegisterViewRequest registerViewRequest(String name)
    {
        return ImmutableRegisterViewRequest.builder()
                .name(name)
                .metadataLocation("file:/view-metadata.json")
                .build();
    }

    public static RenameTableRequest renameRequest(TableIdentifier source, TableIdentifier target)
    {
        return RenameTableRequest.builder()
                .withSource(source)
                .withDestination(target)
                .build();
    }

    public static ReportMetricsRequest reportMetricsRequest()
    {
        return ImmutableReportMetricsRequest.builder()
                .reportType(ReportMetricsRequest.ReportType.UNKNOWN)
                .report(new MetricsReport() {})
                .build();
    }

    public static UpdateTableRequest updateTableRequest()
    {
        return new UpdateTableRequest();
    }

    public static CreateNamespaceRequest createNamespaceRequest()
    {
        return CreateNamespaceRequest.builder().withNamespace(Namespace.of("default")).build();
    }

    public static UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest()
    {
        return new UpdateNamespacePropertiesRequest();
    }

    public static CommitTransactionRequest commitTransactionRequest()
    {
        return new CommitTransactionRequest(ImmutableList.of(
                UpdateTableRequest.create(TableIdentifier.of("ns", "t"), ImmutableList.of(), ImmutableList.of())));
    }

    public static PlanTableScanRequest planTableScanRequest()
    {
        return PlanTableScanRequest.builder().build();
    }

    public static FetchScanTasksRequest fetchScanTasksRequest()
    {
        return new FetchScanTasksRequest("planTask");
    }
}
