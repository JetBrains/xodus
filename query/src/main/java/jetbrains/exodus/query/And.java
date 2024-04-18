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
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class And extends CommutativeOperator {

    private static final boolean traceFindLinks = Boolean.getBoolean("jetbrains.exodus.query.traceFindLinks");

    public And(final NodeBase left, final NodeBase right) {
        super(left, right);
    }

    @Override
    public Iterable<Entity> instantiate(final String entityType,
                                        final QueryEngine queryEngine,
                                        final ModelMetaData metaData,
                                        final InstantiateContext context) {
        final NodeBase left = getLeft();
        final NodeBase right = getRight();
        var leftInstance = left.instantiate(entityType, queryEngine, metaData, context);
        var rightInstance = right.instantiate(entityType, queryEngine, metaData, context);
        if (leftInstance instanceof EntityIterable && rightInstance instanceof EntityIterable){
            return ((EntityIterable) leftInstance).intersect((EntityIterable) rightInstance);
        } else {
            var leftStream = StreamSupport.stream(leftInstance.spliterator(), false);
            var rightSet = StreamSupport.stream(rightInstance.spliterator(), false).collect(Collectors.toSet());
            return leftStream.filter(rightSet::contains).collect(Collectors.toList());
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

    public static NodeBase and(final NodeBase left, final NodeBase right) {
        if (left instanceof GetAll) {
            return right;
        }
        if (right instanceof GetAll) {
            return left;
        }
        return new And(left, right);
    }
}
