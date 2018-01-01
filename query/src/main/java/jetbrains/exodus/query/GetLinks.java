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
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntity;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.query.metadata.ModelMetaData;

import static jetbrains.exodus.query.Utils.safe_equals;

public class GetLinks extends NodeBase {
    private final EntityId id;
    private final String linkName;

    public GetLinks(final EntityId from, final String linkName) {
        id = from;
        this.linkName = linkName;
    }

    @Override
    protected boolean polymorphic() {
        return true;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        if (id != null) {
            final PersistentEntity source = new PersistentEntity(queryEngine.getPersistentStore(), (PersistentEntityId) id);
            return source.getLinks(linkName);
        }
        return EntityIterableBase.EMPTY;
    }

    @Override
    public NodeBase getClone() {
        return new GetLinks(id, linkName);
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
        if (!(obj instanceof GetLinks)) {
            return false;
        }
        GetLinks getLinks = (GetLinks) obj;
        return safe_equals(linkName, getLinks.linkName) && safe_equals(id, getLinks.id);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + id + '.' + linkName + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        final StringBuilder result = super.getHandle(sb).append('(');
        if (id != null) {
            result.append(id);
        }
        return result.append('.').append(linkName).append(')');
    }

    @Override
    public String getSimpleName() {
        return "gl";
    }
}
