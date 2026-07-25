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
package io.trino.plugin.warp.extension.di;

import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.inject.Binder;
import com.google.inject.matcher.Matchers;
import io.airlift.jaxrs.JaxrsBinder;
import io.airlift.jaxrs.JaxrsModule;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.trino.plugin.warp.annotation.Audit;
import io.trino.plugin.warp.extension.execution.CorsFilter;
import io.trino.plugin.warp.extension.execution.WarpExtResource;
import io.trino.plugin.warp.extension.execution.WarpTasksModule;
import io.trino.plugin.warp.util.Auditer;
import io.trino.plugin.warp.util.TrinoExceptionMapper;

import java.util.stream.Collectors;

import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class WarpJaxrsModule
        extends JaxrsModule
{
    private final boolean isCoordinator;
    private final boolean isWorker;

    public WarpJaxrsModule(
            boolean isCoordinator,
            boolean isWorker)
    {
        this.isCoordinator = isCoordinator;
        this.isWorker = isWorker;
    }

    @Override
    public void setup(Binder binder)
    {
        super.setup(binder);

        JsonMapper jsonMapper = new JsonMapper();
        // ignore unknown fields (for backwards compatibility)
        jsonMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // do not allow converting a float to an integer
        jsonMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);

        // use ISO dates
        jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        final DeserializationConfig newDeserializationConfig = jsonMapper.getDeserializationConfig().with(MapperFeature.AUTO_DETECT_CREATORS)
                .with(MapperFeature.AUTO_DETECT_FIELDS)
                .with(MapperFeature.AUTO_DETECT_SETTERS)
                .with(MapperFeature.AUTO_DETECT_GETTERS)
                .with(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .with(MapperFeature.USE_GETTERS_AS_SETTERS)
                .with(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .with(MapperFeature.INFER_PROPERTY_MUTATORS)
                .with(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

        final SerializationConfig newSerializationConfig = jsonMapper.getSerializationConfig().with(MapperFeature.AUTO_DETECT_CREATORS)
                .with(MapperFeature.AUTO_DETECT_FIELDS)
                .with(MapperFeature.AUTO_DETECT_SETTERS)
                .with(MapperFeature.AUTO_DETECT_GETTERS)
                .with(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .with(MapperFeature.USE_GETTERS_AS_SETTERS)
                .with(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .with(MapperFeature.INFER_PROPERTY_MUTATORS)
                .with(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);
        jsonMapper.setConfig(newSerializationConfig);
        jsonMapper.setConfig(newDeserializationConfig);

        jsonMapper.registerModules(
                new JavaTimeModule(),
                new Jdk8Module(),
                new JodaModule(),
                new ParameterNamesModule(),
                new GuavaModule());
        JaxrsBinder.jaxrsBinder(binder).bindInstance(jsonMapper);

        OpenApiResource openApiResource = new OpenApiResource();
        openApiResource.setOpenApiConfiguration(
                new SwaggerConfiguration()
                        .resourceClasses(WarpTasksModule.getTaskExecutors(isCoordinator, isWorker).stream().map(Class::getName).collect(Collectors.toSet())));
        jaxrsBinder(binder).bindInstance(openApiResource);
        jaxrsBinder(binder).bind(WarpExtResource.class);
        jaxrsBinder(binder).bind(TrinoExceptionMapper.class);
        jaxrsBinder(binder).bind(CorsFilter.class);
        httpServerBinder(binder).bindResource("/swagger", "webapp/ui").withWelcomeFile("index.html");
//        httpServerBinder(binder).bindResource("swagger.json", "webapp").withWelcomeFile("swagger.json");
        httpServerBinder(binder).bindResource("openapi.json", "webapp").withWelcomeFile("openapi.json");
        binder.bindInterceptor(Matchers.any(), Matchers.annotatedWith(Audit.class), new Auditer());
    }
}
