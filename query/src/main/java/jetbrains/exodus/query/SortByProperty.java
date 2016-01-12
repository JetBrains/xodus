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
import org.jetbrains.annotations.NotNull;

public class SortByProperty extends Sort {
    private final String propertyName;

    public SortByProperty(final NodeBase child, String propertyName, boolean ascending) {
        super(child, ascending);
        this.propertyName = propertyName;
    }

    @Override
    public NodeBase getClone() {
        return new SortByProperty(getChild().getClone(), propertyName, getAscending());
    }

    @Override
    public boolean canBeCached() {
        return true;
    }

    @Override
    public boolean equalAsSort(Object o) {
        if (!(o instanceof SortByProperty)) {
            return false;
        }
        SortByProperty sort = (SortByProperty) o;
        return getAscending() == sort.getAscending() && eq_dneuad_a0a2a3(propertyName, sort.propertyName);
    }

    @Override
    public Iterable<Entity> applySort(String entityType, Iterable<Entity> iterable, @NotNull final SortEngine sortEngine) {
        return sortEngine.sort(entityType, propertyName, iterable, getAscending());
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('(').append(propertyName).append(',').append(getAscending()).append(')').append('{');
        child.getHandle(sb);
        return sb.append('}');
    }

    @Override
    public String getSimpleName() {
        return "sp";
    }

    private static boolean eq_dneuad_a0a2a3(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }
}
