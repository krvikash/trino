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

import io.trino.spi.type.TypeManager;

public class NoopTypeManager
{
    public static final TypeManager NOOP_TYPE_MANAGER = new TypeManager()
    {
        @Override
        public io.trino.spi.type.Type getType(io.trino.spi.type.TypeSignature signature)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.trino.spi.type.Type fromSqlType(String type)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.trino.spi.type.Type getType(io.trino.spi.type.TypeId id)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.trino.spi.type.TypeOperators getTypeOperators()
        {
            throw new UnsupportedOperationException();
        }
    };

    private NoopTypeManager() {}
}
