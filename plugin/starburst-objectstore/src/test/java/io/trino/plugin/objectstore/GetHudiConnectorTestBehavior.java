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
package io.trino.plugin.objectstore;

import io.trino.plugin.hudi.TestHudiConnectorTest;
import io.trino.testing.TestingConnectorBehavior;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.util.Reflection.methodHandle;

final class GetHudiConnectorTestBehavior
{
    private static final TestHudiConnectorTest TEST_INSTANCE = new TestHudiConnectorTest();
    private static final MethodHandle HAS_BEHAVIOR;

    static {
        try {
            Method method = TestHudiConnectorTest.class.getDeclaredMethod("hasBehavior", TestingConnectorBehavior.class);
            method.setAccessible(true);
            HAS_BEHAVIOR = methodHandle(method).bindTo(TEST_INSTANCE);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        try {
            return (boolean) HAS_BEHAVIOR.invokeExact(connectorBehavior);
        }
        catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
