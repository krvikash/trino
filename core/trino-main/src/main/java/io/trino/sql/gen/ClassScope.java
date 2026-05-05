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
package io.trino.sql.gen;

import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.ParameterizedType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static java.util.Objects.requireNonNull;

public class ClassScope
{
    private final ClassDefinition classDefinition;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final List<FieldDefinition> contextFields = new ArrayList<>();
    private final Map<ParameterizedType, Deque<FieldDefinition>> releasedContextFields = new HashMap<>();

    private int nextId;
    private int nextContextFieldId;

    public ClassScope(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder)
    {
        this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
        this.cachedInstanceBinder = requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
    }

    public FieldDefinition getOrCreateContextField(ParameterizedType type)
    {
        Deque<FieldDefinition> typeFields = releasedContextFields.get(type);
        if (typeFields == null || typeFields.isEmpty()) {
            FieldDefinition field = classDefinition.declareField(a(PUBLIC), "__context_" + nextContextFieldId, type);
            nextContextFieldId++;
            contextFields.add(field);
            return field;
        }
        return typeFields.pop();
    }

    public void releaseAllContextFields()
    {
        for (FieldDefinition field : contextFields) {
            releasedContextFields.computeIfAbsent(field.getType(), _ -> new ArrayDeque<>()).push(field);
        }
    }

    public int getNextId()
    {
        return nextId++;
    }

    public ClassDefinition classDefinition()
    {
        return classDefinition;
    }

    public CachedInstanceBinder cachedInstanceBinder()
    {
        return cachedInstanceBinder;
    }
}
