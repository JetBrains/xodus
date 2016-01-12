/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.metadata.ModelMetaData;

public class PropertyRange extends NodeBase {
    private final String name;
    private final Comparable min;
    private final Comparable max;

    public PropertyRange(String name, Comparable min, Comparable max) {
        this.name = name;
        this.min = min;
        this.max = max;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        queryEngine.assertOperational();
        return queryEngine.getPersistentStore().getAndCheckCurrentTransaction().find(entityType, name, min, max);
    }

    PropertyRange merge(PropertyRange range) {
        return new PropertyRange(name,
                min.compareTo(range.min) < 0 ? range.min : min,
                max.compareTo(range.max) < 0 ? max : range.max
        );
    }

    String getPropertyName() {
        return name;
    }

    @Override
    public NodeBase getClone() {
        return new PropertyRange(name, min, max);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        checkWildcard(obj);
        if (!(obj instanceof PropertyRange)) {
            return false;
        }
        PropertyRange propertyRange = (PropertyRange) obj;
        return eq_xl77s4_a0a0e0e(name, propertyRange.name) && eq_xl77s4_a0a0e0e_0(min, propertyRange.min) && eq_xl77s4_a0a4a4(max, propertyRange.max);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + min + "<=" + name + "<=" + max + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        return super.getHandle(sb).append('(').append(min).append('<').append(name).append('<').append(max).append(')');
    }

    @Override
    public String getSimpleName() {
        return "pr";
    }

    private static boolean eq_xl77s4_a0a0e0e(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }

    private static boolean eq_xl77s4_a0a0e0e_0(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }

    private static boolean eq_xl77s4_a0a4a4(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }
}
