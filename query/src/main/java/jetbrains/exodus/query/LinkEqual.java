/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.orientdb.OEntity;
import jetbrains.exodus.entitystore.orientdb.iterate.link.OLinkToEntityIterable;
import jetbrains.exodus.query.metadata.ModelMetaData;

import static jetbrains.exodus.query.Utils.safe_equals;

public class LinkEqual extends NodeBase {

    private final String name;
    private final EntityId id;
    private final Entity entity;

    public LinkEqual(String name, Entity to) {
        this(name, to == null ? null : to.getId(), to);
    }

    private LinkEqual(String name, EntityId id, Entity entity) {
        this.name = name;
        this.id = id;
        this.entity = entity;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        var txn = queryEngine.getPersistentStore().getAndCheckCurrentTransaction();
        return new OLinkToEntityIterable(txn, name, ((OEntity) entity).getId());
    }

    @Override
    public NodeBase getClone() {
        return new LinkEqual(name, id, entity);
    }

    public String getName() {
        return name;
    }

    public EntityId getToId() {
        return id;
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
        if (!(obj instanceof LinkEqual)) {
            return false;
        }
        LinkEqual linkEqual = (LinkEqual) obj;
        return safe_equals(name, linkEqual.name) && safe_equals(id, linkEqual.id);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + name + '=' + id + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        final StringBuilder result = super.getHandle(sb).append('(').append(name).append('=');
        if (id != null) {
            if (id instanceof PersistentEntityId) {
                ((PersistentEntityId) id).toString(result);
            } else {
                result.append(id);
            }
        }
        return result.append(')');
    }

    @Override
    public String getSimpleName() {
        return "le";
    }
}
