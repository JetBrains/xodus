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
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class Sorts {
    private List<Sort> sorts;

    public void addSort(Sort sort) {
        if (sorts == null) {
            sorts = new ArrayList<>(Sort.MAX_NESTED_SORTS);
        }
        if (sorts.size() < Sort.MAX_NESTED_SORTS) {
            sorts.add(0, sort);
        }
    }

    public Iterable<Entity> apply(String entityType, Iterable<Entity> iterable, @NotNull final QueryEngine queryEngine) {
        if (sorts != null) {
            final SortEngine sortEngine = queryEngine.getSortEngine();
            if (sortEngine == null) {
                throw new UnsupportedOperationException("Sort engine not provided!");
            }
            for (final Sort sort : sorts) {
                iterable = sort.applySort(entityType, iterable, sortEngine);
            }
        }
        return iterable;
    }

    public int sortCount() {
        return sorts == null ? 0 : sorts.size();
    }

    public boolean canBeCached() {
        final List<Sort> sorts = this.sorts;
        if (sorts != null) {
            for (final Sort sort : sorts) {
                if (!(sort.canBeCached())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Sorts)) {
            return false;
        }
        Sorts s = (Sorts) obj;
        if (sortCount() != s.sortCount()) {
            return false;
        }
        if (sorts != null) {
            for (int i = 0; i < sorts.size(); i++) {
                if (!sorts.get(i).equalAsSort(s.sorts.get(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final Sort it : sorts) {
            sb.append(it.getHandle()).append(' ');
        }
        return sb.toString();
    }
}
