/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.entitystore.ComparableGetter;
import jetbrains.exodus.entitystore.Entity;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.query.Utils.safe_equals;

public class ComparableGetterSort extends Sort {
    private final ComparableGetter valueGetter;

    protected ComparableGetterSort(final NodeBase child, ComparableGetter valueGetter, boolean ascending) {
        super(child, ascending);
        this.valueGetter = valueGetter;
    }

    public ComparableGetter getValueGetter() {
        return valueGetter;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    public NodeBase getClone() {
        return new ComparableGetterSort(getChild().getClone(), valueGetter, getAscending());
    }

    @Override
    public boolean equalAsSort(Object o) {
        if (!(o instanceof ComparableGetterSort)) {
            return false;
        }
        ComparableGetterSort sort = (ComparableGetterSort) o;
        return safe_equals(valueGetter, sort.valueGetter) && getAscending() == sort.getAscending();
    }

    @Override
    public Iterable<Entity> applySort(String entityType, Iterable<Entity> iterable, @NotNull SortEngine sortEngine) {
        return sortEngine.sortInMemory(iterable, valueGetter, getAscending());
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('(').append(valueGetter).append(' ').append(getAscending()).append(')').append('{');
        child.getHandle(sb);
        return sb.append('}');
    }

    @Override
    public String getSimpleName() {
        return "cgs";
    }

    public static ComparableGetterSort create(final NodeBase child, ComparableGetter valueGetter, boolean ascending) {
        return new ComparableGetterSort(child, valueGetter, ascending);
    }
}
