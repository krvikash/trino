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

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.objectstore.MethodHandles.translateArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMethodHandles
{
    @Test
    public void testTranslateArgument()
    {
        MethodHandle methodHandle = translateArguments(SomeObject.SET_STRING_INTEGER, String.class, s -> s + "-translated").orElseThrow();
        SomeObject someObject = new SomeObject();
        try {
            methodHandle.invoke(someObject, "something", 42);
        }
        catch (Throwable e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        assertThat(someObject.aString).isEqualTo("something-translated");
        assertThat(someObject.anInteger).isEqualTo(42);
        assertThat(someObject.aLong).isNull();
    }

    @Test
    public void testNoTranslationWhenNoMatchingArgument()
    {
        assertThat(translateArguments(SomeObject.SET_LONG_INTEGER, String.class, s -> s + "translated"))
                .isEmpty();
    }

    static class SomeObject
    {
        private static final MethodHandle SET_STRING_INTEGER;
        private static final MethodHandle SET_LONG_INTEGER;

        static {
            try {
                SET_STRING_INTEGER = lookup().unreflect(SomeObject.class.getMethod("setStringInteger", String.class, Integer.class));
                SET_LONG_INTEGER = lookup().unreflect(SomeObject.class.getMethod("setLongInteger", Long.class, Integer.class));
            }
            catch (ReflectiveOperationException e) {
                throw new LinkageError(e.getMessage(), e);
            }
        }

        private Integer anInteger;
        private String aString;
        private Long aLong;

        public void setStringInteger(String aString, Integer anInteger)
        {
            this.aString = aString;
            this.anInteger = anInteger;
        }

        public void setLongInteger(Long aLong, Integer anInteger)
        {
            this.aLong = aLong;
            this.anInteger = anInteger;
        }
    }
}
