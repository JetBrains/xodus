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

class Wildcard extends NodeBase {
    private final int type;

    /**
     * Create a wildcard node of type {@code t}.
     * Nodes of equal types require matched nodes to be equal.
     *
     * @param t type
     */
    Wildcard(int t) {
        type = t;
    }

    public int getType() {
        return type;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        throw new UnsupportedOperationException("Can't instantiate wildcard node.");
    }

    @Override
    public NodeBase getClone() {
        return new Wildcard(type);
    }

    @Override
    String getHandle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }

    public int hashCode() {
        return type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Wildcard)) {
            throw new RuntimeException("Can't compare wildcard with " + obj.getClass() + '.');
        }
        Wildcard wildcard = (Wildcard) obj;
        return type == wildcard.type;
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + "(type=" + type + ") ";
    }

    @Override
    public String getSimpleName() {
        return "w";
    }
}
