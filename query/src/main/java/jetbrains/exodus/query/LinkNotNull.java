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
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.query.metadata.ModelMetaData;

import static jetbrains.exodus.query.Utils.safe_equals;

public class LinkNotNull extends NodeBase {
    private final String name;

    public LinkNotNull(String name) {
        this.name = name;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        queryEngine.assertOperational();
        final PersistentStoreTransaction txn = queryEngine.getPersistentStore().getAndCheckCurrentTransaction();
        return txn.findWithLinks(entityType, name);
    }

    @Override
    public NodeBase getClone() {
        return new LinkNotNull(name);
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
        if (!(obj instanceof LinkNotNull)) {
            return false;
        }
        LinkNotNull linkNotNull = (LinkNotNull) obj;
        return safe_equals(name, linkNotNull.name);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + name + "!=null) ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        return super.getHandle(sb).append('(').append(name).append(')');
    }

    @Override
    public String getSimpleName() {
        return "lnn";
    }
}
