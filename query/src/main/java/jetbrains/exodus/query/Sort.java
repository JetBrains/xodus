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
import org.jetbrains.annotations.NotNull;

public abstract class Sort extends UnaryNode {
    static final int MAX_NESTED_SORTS = 4;

    private final boolean ascending;

    protected Sort(final NodeBase child, boolean ascending) {
        super(child == null ? NodeFactory.all() : child);
        this.ascending = ascending;
    }

    public boolean getAscending() {
        return ascending;
    }

    public abstract boolean canBeCached();

    @Override
    public void optimize(Sorts sorts, OptimizationPlan rules) {
        sorts.addSort(this);
        NodeBase parent = getParent();
        parent.replaceChild(this, getChild());
        getChild().optimize(sorts, rules);
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData) {
        throw new RuntimeException(getClass() + " node in optimized tree.");
    }

    public abstract Iterable<Entity> applySort(String entityType, Iterable<Entity> iterable, @NotNull final SortEngine sortEngine);

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        checkWildcard(obj);
        return equalAsSort(obj) && super.equals(obj);
    }

    public abstract boolean equalAsSort(Object o);
}
