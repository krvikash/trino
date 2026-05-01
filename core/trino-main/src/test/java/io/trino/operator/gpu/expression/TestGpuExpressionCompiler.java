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
package io.trino.operator.gpu.expression;

import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.testing.InterfaceTestUtils;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isProtected;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @see io.trino.operator.gpu.TestGpuExpressions for tests covering expression execution
 */
class TestGpuExpressionCompiler
{
    @Test
    void testEveryExpressionConsidered()
            throws Exception
    {
        assertAllMethodsOverridden(IrVisitor.class, GpuExpressionCompiler.CompilationVisitor.class, Set.of(
                // has good default
                IrVisitor.class.getMethod("process", Expression.class, Object.class /*context*/)));
    }

    /**
     * Like {@link io.trino.testing.InterfaceTestUtils#assertAllMethodsOverridden(Class, Class, Set)} but also works with abstract classes and protected methods
     */
    public static <I, C extends I> void assertAllMethodsOverridden(Class<I> superType, Class<C> clazz, Set<Method> exclusions)
    {
        checkArgument(superType.isAssignableFrom(clazz), "%s is not supertype of %s", superType, clazz);
        exclusions = new HashSet<>(exclusions);
        for (Class<?> parent = superType; parent != null; parent = parent.getSuperclass()) {
            for (Method method : parent.getDeclaredMethods()) {
                if (isStatic(method.getModifiers())) {
                    continue;
                }
                if (!(isPublic(method.getModifiers()) || isProtected(method.getModifiers()))) {
                    continue;
                }
                if (method.getDeclaringClass() == Object.class) {
                    continue;
                }
                try {
                    Method override = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                    if (!method.getReturnType().isAssignableFrom(override.getReturnType())) {
                        Fail.fail(format("%s is not assignable from %s for method %s", method.getReturnType(), override.getReturnType(), method));
                    }
                }
                catch (NoSuchMethodException e) {
                    if (exclusions.remove(method)) {
                        // ignored
                    }
                    else {
                        Fail.fail(format("%s does not override [%s]", clazz.getName(), method));
                    }
                }
            }
        }

        if (!exclusions.isEmpty()) {
            Fail.fail("Following exclusions are redundant: " + exclusions);
        }
    }

    @Test
    void remindToDeleteAssertAllMethodsOverriddenCopy()
    {
        assertThatThrownBy(() -> assertAllMethodsOverridden(AbstractClass.class, ConcreteClass.class, Set.of()))
                .hasMessage("io.trino.operator.gpu.expression.TestGpuExpressionCompiler$ConcreteClass does not override [protected void io.trino.operator.gpu.expression.TestGpuExpressionCompiler$AbstractClass.overrideMe()]");

        // TODO When InterfaceTestUtils.assertAllMethodsOverridden is changed so that it also fails (e.g. https://github.com/trinodb/trino/pull/29300), remove our enhanced copy of assertAllMethodsOverridden
        InterfaceTestUtils.assertAllMethodsOverridden(AbstractClass.class, ConcreteClass.class);
    }

    private abstract static class AbstractClass
    {
        protected void overrideMe() {}
    }

    private static class ConcreteClass
            extends AbstractClass {}
}
