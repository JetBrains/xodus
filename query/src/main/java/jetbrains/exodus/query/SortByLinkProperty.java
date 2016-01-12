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

public class SortByLinkProperty extends Sort {
    private final String enumType;
    private final String propName;
    private final String linkName;

    public SortByLinkProperty(final NodeBase child, String enumType, String propName, String linkName, boolean ascending) {
        super(child, ascending);
        this.enumType = enumType;
        this.propName = propName;
        this.linkName = linkName;
    }

    @Override
    public NodeBase getClone() {
        return new SortByLinkProperty(getChild().getClone(), enumType, propName, linkName, getAscending());
    }

    @Override
    public boolean canBeCached() {
        return true;
    }

    @Override
    public boolean equalAsSort(Object o) {
        if (!(o instanceof SortByLinkProperty)) {
            return false;
        }
        SortByLinkProperty sort = (SortByLinkProperty) o;
        return getAscending() == sort.getAscending() && eq_wfo59e_a0a0a2a3(enumType, sort.enumType) && eq_wfo59e_a0a0c0d(propName, sort.propName) && eq_wfo59e_a0a2a3(linkName, sort.linkName);
    }

    @Override
    public Iterable<Entity> applySort(String entityType, Iterable<Entity> iterable, @NotNull final SortEngine sortEngine) {
        return sortEngine.sort(enumType, propName, entityType, linkName, iterable, getAscending());
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        super.getHandle(sb).append('(').append(enumType).append(',').append(propName).append(',').append(linkName).append(',').append(getAscending()).append(')').append('{');
        child.getHandle(sb);
        return sb.append('}');
    }

    @Override
    public String getSimpleName() {
        return "slp";
    }

    private static boolean eq_wfo59e_a0e0c(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }

    private static boolean eq_wfo59e_a0a0a2a3(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }

    private static boolean eq_wfo59e_a0a0c0d(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }

    private static boolean eq_wfo59e_a0a2a3(Object a, Object b) {
        return a != null ? a.equals(b) : a == b;
    }
}
