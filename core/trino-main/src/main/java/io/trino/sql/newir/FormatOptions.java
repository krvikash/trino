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
package io.trino.sql.newir;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static java.util.Objects.requireNonNull;

public class FormatOptions
{
    public static final String INDENT = "    ";
    private static final int CURRENT_VERSION = 1;

    public static final FormatOptions TESTING_FORMAT_OPTIONS = new FormatOptions(TESTING_DIALECT_REGISTRY);
    public static final PrintOptions TESTING_PRINT_OPTIONS = TESTING_FORMAT_OPTIONS.printOptions();

    private final DialectRegistry dialectRegistry;

    @Inject
    public FormatOptions(DialectRegistry dialectRegistry)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
    }

    public static void validateVersion(int version)
    {
        if (version != 1) {
            throw new TrinoException(IR_ERROR, "invalid format version: " + version);
        }
    }

    public PrintOptions printOptions(int version)
    {
        return new PrintOptions(version, dialectRegistry);
    }

    public PrintOptions printOptions()
    {
        return new PrintOptions(dialectRegistry);
    }

    public static class PrintOptions
    {
        private final int version;
        private final DialectRegistry dialectRegistry;

        private PrintOptions(DialectRegistry dialectRegistry)
        {
            this(CURRENT_VERSION, dialectRegistry);
        }

        private PrintOptions(int version, DialectRegistry dialectRegistry)
        {
            validateVersion(version);
            this.version = version;
            this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
        }

        public int version()
        {
            return version;
        }

        public String formatName(Operation operation)
        {
            if (version == 1 && operation.dialect().equals(TRINO)) {
                return operation.name();
            }
            return operation.dialect() + "." + operation.name();
        }

        public String formatAttribute(AttributeKey key, Object attribute)
        {
            String dialectPrefix;
            if (version == 1 && key.dialect().equals(TRINO)) {
                dialectPrefix = "";
            }
            else {
                dialectPrefix = key.dialect() + ".";
            }

            return dialectPrefix + key.name() + " = " + quote(dialectRegistry.dialect(key.dialect()).formatAttribute(key.name(), attribute));
        }

        public String formatType(Type type)
        {
            String dialectPrefix;
            if (version == 1 && type.dialect().equals(TRINO)) {
                dialectPrefix = "";
            }
            else {
                dialectPrefix = type.dialect() + ".";
            }

            return dialectPrefix + quote(dialectRegistry.dialect(type.dialect()).formatType(type));
        }

        private static String quote(String string)
        {
            return "\"" + string.replace("\"", "\"\"") + "\"";
        }
    }

    public ParseOptions parseOptions(int version)
    {
        return new ParseOptions(version, dialectRegistry);
    }

    public ParseOptions parseOptions()
    {
        return new ParseOptions(dialectRegistry);
    }

    public static class ParseOptions
    {
        private final int version;
        private final DialectRegistry dialectRegistry;

        private ParseOptions(DialectRegistry dialectRegistry)
        {
            this(CURRENT_VERSION, dialectRegistry);
        }

        private ParseOptions(int version, DialectRegistry dialectRegistry)
        {
            validateVersion(version);
            this.version = version;
            this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
        }

        public int version()
        {
            return version;
        }

        public Object parseAttribute(Optional<String> dialect, String name, String attribute)
        {
            String dialectName;
            if (version == 1) {
                dialectName = dialect.orElse(TRINO);
            }
            else {
                dialectName = dialect.orElseThrow(() -> new TrinoException(IR_ERROR, "missing dialect name for an attribute in IR version " + version));
            }

            return dialectRegistry.dialect(dialectName).parseAttribute(name, unquote(attribute));
        }

        public Type parseType(Optional<String> dialect, String type)
        {
            String dialectName;
            if (version == 1) {
                dialectName = dialect.orElse(TRINO);
            }
            else {
                dialectName = dialect.orElseThrow(() -> new TrinoException(IR_ERROR, "missing dialect name for a type in IR version " + version));
            }

            return dialectRegistry.dialect(dialectName).parseType(unquote(type));
        }

        private static String unquote(String string)
        {
            return string.substring(1, string.length() - 1)
                    .replace("\"\"", "\"");
        }
    }
}
