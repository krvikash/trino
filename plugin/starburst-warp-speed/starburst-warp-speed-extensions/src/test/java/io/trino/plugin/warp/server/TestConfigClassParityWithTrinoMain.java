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
package io.trino.plugin.warp.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that mirrored config classes in this module stay structurally in sync with the
 * trino-main originals. Property-name drift between warp client and Trino server is silent
 * broken auth, so this test fails the build the moment upstream adds, renames, or removes
 * a property without the warp mirror following along.
 */
public class TestConfigClassParityWithTrinoMain
{
    @Test
    public void testInternalCommunicationConfigParity()
    {
        assertParity(
                io.trino.plugin.warp.server.InternalCommunicationConfig.class,
                io.trino.server.InternalCommunicationConfig.class);
    }

    @Test
    public void testSecurityConfigParity()
    {
        assertParity(
                io.trino.plugin.warp.server.security.SecurityConfig.class,
                io.trino.server.security.SecurityConfig.class);
    }

    private static void assertParity(Class<?> warpClass, Class<?> trinoMainClass)
    {
        assertThat(configProperties(warpClass))
                .as("@Config setters parity (%s vs %s)", warpClass.getName(), trinoMainClass.getName())
                .isEqualTo(configProperties(trinoMainClass));

        assertThat(legacyConfigProperties(warpClass))
                .as("@LegacyConfig setters parity (%s vs %s)", warpClass.getName(), trinoMainClass.getName())
                .isEqualTo(legacyConfigProperties(trinoMainClass));

        assertThat(securitySensitiveProperties(warpClass))
                .as("@ConfigSecuritySensitive setters parity (%s vs %s)", warpClass.getName(), trinoMainClass.getName())
                .isEqualTo(securitySensitiveProperties(trinoMainClass));

        assertThat(defunctProperties(warpClass))
                .as("@DefunctConfig values parity (%s vs %s)", warpClass.getName(), trinoMainClass.getName())
                .isEqualTo(defunctProperties(trinoMainClass));

        assertThat(validationAnnotations(warpClass))
                .as("validation annotation parity (%s vs %s)", warpClass.getName(), trinoMainClass.getName())
                .isEqualTo(validationAnnotations(trinoMainClass));
    }

    /**
     * Property name → "{paramType}|{description}" so that diffs surface a renamed type
     * (e.g. boolean → enum) or a changed description, not just an added/removed property.
     */
    private static Map<String, String> configProperties(Class<?> klass)
    {
        Map<String, String> result = new TreeMap<>();
        for (Method method : klass.getMethods()) {
            Config config = method.getAnnotation(Config.class);
            if (config == null) {
                continue;
            }
            String signature = method.getParameterTypes()[0].getCanonicalName() + "|" + descriptionOf(method);
            String name = config.value();
            if (result.put(name, signature) != null) {
                throw new AssertionError("Duplicate @Config name " + name + " on " + klass.getName());
            }
        }
        return result;
    }

    private static Map<String, String> legacyConfigProperties(Class<?> klass)
    {
        Map<String, String> result = new TreeMap<>();
        for (Method method : klass.getMethods()) {
            LegacyConfig legacy = method.getAnnotation(LegacyConfig.class);
            if (legacy == null) {
                continue;
            }
            String signature = method.getParameterTypes()[0].getCanonicalName();
            for (String name : legacy.value()) {
                if (result.put(name, signature) != null) {
                    throw new AssertionError("Duplicate @LegacyConfig name " + name + " on " + klass.getName());
                }
            }
        }
        return result;
    }

    private static Set<String> securitySensitiveProperties(Class<?> klass)
    {
        Set<String> result = new TreeSet<>();
        for (Method method : klass.getMethods()) {
            if (method.getAnnotation(ConfigSecuritySensitive.class) == null) {
                continue;
            }
            Config config = method.getAnnotation(Config.class);
            if (config != null) {
                result.add(config.value());
            }
        }
        return result;
    }

    private static Set<String> defunctProperties(Class<?> klass)
    {
        DefunctConfig annotation = klass.getAnnotation(DefunctConfig.class);
        if (annotation == null) {
            return Set.of();
        }
        return new TreeSet<>(Set.of(annotation.value()));
    }

    private static String descriptionOf(Method method)
    {
        ConfigDescription description = method.getAnnotation(ConfigDescription.class);
        return description == null ? "" : description.value();
    }

    /**
     * Method name → sorted set of validation annotation type names. Drift in jakarta.validation
     * or airlift FileExists / MinDuration / etc. annotations between warp and trino-main means
     * one side enforces a constraint the other doesn't — e.g. trino-main rejecting an empty
     * shared secret while warp silently accepts it.
     */
    private static Map<String, Set<String>> validationAnnotations(Class<?> klass)
    {
        Map<String, Set<String>> result = new TreeMap<>();
        for (Method method : klass.getMethods()) {
            Set<String> annotations = new TreeSet<>();
            for (Annotation annotation : method.getAnnotations()) {
                String name = annotation.annotationType().getName();
                if (name.startsWith("jakarta.validation.constraints.")
                        || name.startsWith("io.airlift.configuration.validation.")) {
                    annotations.add(name);
                }
            }
            if (!annotations.isEmpty()) {
                result.put(method.getName(), annotations);
            }
        }
        return result;
    }
}
