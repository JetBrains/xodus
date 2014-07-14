/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.metadata.ModelMetaData;

public class And extends CommutativeOperator {

    public And(final NodeBase left, final NodeBase right) {
        super(left, right);
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        final NodeBase left = getLeft();
        final NodeBase right = getRight();
        if (left instanceof LinksEqualDecorator) {
            final LinksEqualDecorator leftNode = (LinksEqualDecorator) left;
            final Iterable<Entity> rightInstance = right.instantiate(entityType, queryEngine, metaData);
            if (rightInstance instanceof EntityIterableBase) {
                return ((EntityIterableBase) rightInstance).findLinks(
                        ((EntityIterable) leftNode.getDecorated().instantiate(leftNode.getLinkEntityType(), queryEngine, metaData)).getSource(),
                        leftNode.getLinkName());
            }
        } else if (right instanceof LinksEqualDecorator) {
            final LinksEqualDecorator rightNode = (LinksEqualDecorator) right;
            final Iterable<Entity> leftInstance = right.instantiate(entityType, queryEngine, metaData);
            if (leftInstance instanceof EntityIterableBase) {
                return ((EntityIterableBase) leftInstance).findLinks(
                        ((EntityIterable) rightNode.getDecorated().instantiate(rightNode.getLinkEntityType(), queryEngine, metaData)).getSource(),
                        rightNode.getLinkName());
            }
        }
        return queryEngine.intersectAdjusted(left.instantiate(entityType, queryEngine, metaData), right.instantiate(entityType, queryEngine, metaData));
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
        return obj instanceof And && super.equals(obj);
    }

    @Override
    public NodeBase getClone() {
        return new And(getLeft().getClone(), getRight().getClone());
    }

    @Override
    public String getSimpleName() {
        return "and";
    }
}
