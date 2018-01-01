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


/**
 * {@code isOk()} on the matched node must be true.
 * Matched node is substituted by {@code convert(matched)}.
 */
abstract class ConversionWildcard<E extends NodeBase> extends NodeBase {
    final int type;

    protected ConversionWildcard(final int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public abstract boolean isOk(E node);

    public abstract NodeBase convert(E matched);

    public abstract Class getClazz();

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        throw new UnsupportedOperationException("Can't instantiate wildcard node.");
    }

    public abstract boolean equals(Object o);

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + "(type=" + type + ") ";
    }

    public int hashCode() {
        return (getClass() + String.valueOf(type)).hashCode();
    }

    @Override
    String getHandle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }
}
