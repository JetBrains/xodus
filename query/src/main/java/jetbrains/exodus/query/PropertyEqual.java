/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.query.Utils.safe_equals;

public class PropertyEqual extends NodeBase {

    private final String name;
    @Nullable
    private final Comparable value;

    public PropertyEqual(String name, @Nullable Comparable value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Comparable getValue() {
        return value;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        queryEngine.assertOperational();
        return queryEngine.getPersistentStore().getAndCheckCurrentTransaction().find(entityType, name, value);
    }

    @Override
    public NodeBase getClone() {
        return new PropertyEqual(name, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        checkWildcard(obj);
        if (!(obj instanceof PropertyEqual)) {
            return false;
        }
        PropertyEqual propertyEqual = (PropertyEqual) obj;
        return safe_equals(name, propertyEqual.name) && safe_equals(value, propertyEqual.value);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + name + '=' + value + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        return super.getHandle(sb).append('(').append(name).append('=').append(value).append(')');
    }

    @Override
    public String getSimpleName() {
        return "pe";
    }
}
