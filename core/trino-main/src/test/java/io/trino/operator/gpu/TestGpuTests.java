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
package io.trino.operator.gpu;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static java.lang.reflect.Modifier.isStatic;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuTests
{
    private static final String MEMORY_POOL_SETUP_METHOD = "maybeSetGpuMemoryPool";

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    /**
     * The first RMM initialization in a JVM wins, so a GPU test class that runs first without
     * setting the pool leaves all later test classes in that JVM without it.
     */
    @Test
    void testAllGpuTestsSetGpuMemoryPool()
            throws IOException
    {
        List<Class<?>> testClasses = ClassPath.from(getClass().getClassLoader())
                .getTopLevelClassesRecursive(getClass().getPackageName()).stream()
                .filter(classInfo -> isTestClassName(classInfo.getSimpleName()))
                .map(ClassInfo::load)
                .collect(toImmutableList());
        assertThat(testClasses)
                .as("GPU test classes found on the classpath")
                .contains(TestGpuTests.class)
                .hasSizeGreaterThan(1);

        List<String> missingSetup = testClasses.stream()
                .filter(testClass -> !hasMemoryPoolSetup(testClass))
                .map(Class::getName)
                .sorted()
                .collect(toImmutableList());
        assertThat(missingSetup)
                .as("GPU test classes without a static @BeforeAll %s() method calling GpuTestUtils.maybeSetGpuMemoryPoolForTests()", MEMORY_POOL_SETUP_METHOD)
                .isEmpty();
    }

    private static boolean isTestClassName(String simpleName)
    {
        return simpleName.startsWith("Test") && !simpleName.startsWith("Testing") && !simpleName.startsWith("Tested");
    }

    private static boolean hasMemoryPoolSetup(Class<?> testClass)
    {
        for (Class<?> clazz = testClass; clazz != null; clazz = clazz.getSuperclass()) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().equals(MEMORY_POOL_SETUP_METHOD)
                        && method.getParameterCount() == 0
                        && isStatic(method.getModifiers())
                        && method.isAnnotationPresent(BeforeAll.class)) {
                    return true;
                }
            }
        }
        return false;
    }
}
