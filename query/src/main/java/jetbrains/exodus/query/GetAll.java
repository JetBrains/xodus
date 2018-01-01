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


import jetbrains.exodus.core.dataStructures.NanoSet;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.metadata.ModelMetaData;

import java.util.Collection;

public class GetAll extends NodeBase {

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        queryEngine.assertOperational();
        return queryEngine.instantiateGetAll(entityType);
    }

    @Override
    public NodeBase getClone() {
        return NodeFactory.all();
    }

    @Override
    public NodeBase replaceChild(NodeBase child, NodeBase newChild) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<NodeBase> getChildren() {
        return NodeBase.NO_CHILDREN;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Iterable<NodeBase> getDescendants() {
        return new NanoSet<NodeBase>(this);
    }

    @Override
    public String getSimpleName() {
        return "all";
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
        return obj instanceof GetAll;
    }
}
