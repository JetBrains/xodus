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
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.query.metadata.EntityMetaData;
import jetbrains.exodus.query.metadata.ModelMetaData;

import static jetbrains.exodus.query.Utils.safe_equals;

public class LinksEqualDecorator extends NodeBase {
    private final String name;
    private NodeBase decorated;
    private final String decoratedEntityType;

    public LinksEqualDecorator(String name, NodeBase decorated, String decoratedEntityType) {
        this.name = name;
        this.decorated = decorated;
        this.decoratedEntityType = decoratedEntityType;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        queryEngine.assertOperational();
        return queryEngine.getPersistentStore().getAndCheckCurrentTransaction().findLinks(entityType,
                (EntityIterable) instantiateDecorated(decoratedEntityType, queryEngine, metaData), name);
    }

    @Override
    public NodeBase getClone() {
        return new LinksEqualDecorator(name, decorated.getClone(), decoratedEntityType);
    }

    public NodeBase getDecorated() {
        return decorated;
    }

    public void setDecorated(NodeBase decorated) {
        this.decorated = decorated;
    }

    protected Iterable<Entity> instantiateDecorated(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        Iterable<Entity> result = decorated.instantiate(entityType, queryEngine, metaData);
        final EntityMetaData emd = metaData == null ? null : metaData.getEntityMetaData(entityType);
        if (emd != null) {
            for (String subType : emd.getSubTypes()) {
                result = queryEngine.unionAdjusted(result, instantiateDecorated(subType, queryEngine, metaData));
            }
        }
        return result;
    }

    @Override
    public void optimize(Sorts sorts, OptimizationPlan rules) {
        if (decorated instanceof LinkEqual) {
            final LinkEqual linkEqual = (LinkEqual) decorated;
            if (linkEqual.getToId() == null) {
                setDecorated(new Minus(NodeFactory.all(), new LinkNotNull(linkEqual.getName())));
            }
        } else if (decorated instanceof PropertyEqual) {
            final PropertyEqual propEqual = (PropertyEqual) decorated;
            if (propEqual.getValue() == null) {
                setDecorated(new Minus(NodeFactory.all(), new PropertyNotNull(propEqual.getName())));
            }
        }
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
        if (!(obj instanceof LinksEqualDecorator)) {
            return false;
        }
        LinksEqualDecorator decorator = (LinksEqualDecorator) obj;
        if (!safe_equals(name, decorator.name) || !safe_equals(decoratedEntityType, decorator.decoratedEntityType)) {
            return false;
        }
        return safe_equals(decorated, decorator.decorated);
    }

    @Override
    protected String toString(String prefix) {
        return super.toString(prefix) + '\n' + decorated.toString(TREE_LEVEL_INDENT + prefix);
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('(').append(name).append(',').append(decoratedEntityType).append(')').append('{');
        return decorated.getHandle(sb).append('}');
    }

    @Override
    public String getSimpleName() {
        return "led";
    }

    public String getLinkName() {
        return name;
    }

    public String getLinkEntityType() {
        return decoratedEntityType;
    }
}
