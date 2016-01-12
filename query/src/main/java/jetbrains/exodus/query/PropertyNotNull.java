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
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.metadata.EntityMetaData;
import jetbrains.exodus.entitystore.metadata.ModelMetaData;
import jetbrains.exodus.entitystore.metadata.PropertyMetaData;
import jetbrains.exodus.entitystore.metadata.PropertyType;

public class PropertyNotNull extends NodeBase {
    private final String name;

    public PropertyNotNull(String name) {
        this.name = name;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        final EntityMetaData emd = metaData == null ? null : metaData.getEntityMetaData(entityType);
        final PropertyMetaData pmd = emd == null ? null : emd.getPropertyMetaData(name);
        queryEngine.assertOperational();
        final PersistentStoreTransaction txn = queryEngine.getPersistentStore().getAndCheckCurrentTransaction();
        return pmd == null || pmd.getType() == PropertyType.PRIMITIVE ?
                txn.findWithProp(entityType, name) : txn.findWithBlob(entityType, name);
    }

    @Override
    public NodeBase getClone() {
        return new PropertyNotNull(name);
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
        if (!(obj instanceof PropertyNotNull)) {
            return false;
        }
        PropertyNotNull propertyNotNull = (PropertyNotNull) obj;
        return eq_4wg6n6_a0e0c(name, propertyNotNull.name);
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
        return "pnn";
    }

    private static boolean eq_4wg6n6_a0e0c(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }
}
