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
package io.trino.plugin.base.security.passthrough;

import com.google.common.base.CharMatcher;
import org.junit.jupiter.api.Test;

import static com.google.common.base.CharMatcher.anyOf;
import static com.google.common.base.CharMatcher.inRange;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestIdPName
{
    @Test
    public void testValidName()
    {
        assertThatNoException()
                .isThrownBy(() -> IdPName.of("abcdefghijklmnoprstquwxyz1234567890-_"));
    }

    @Test
    public void testAllInvalidChars()
    {
        CharMatcher allInvalid = inRange('a', 'z').or(inRange('0', '9')).or(anyOf("-_")).negate();
        for (int i = 0; i < 127; i++) {
            char testedChar = (char) i;
            if (allInvalid.matches(testedChar)) {
                assertThatIllegalArgumentException()
                        .describedAs("Expecting IllegalArgumentException to be thrown, when '%s' is used in name", testedChar)
                        .isThrownBy(() -> IdPName.of(String.valueOf(testedChar)));
            }
        }
    }
}
