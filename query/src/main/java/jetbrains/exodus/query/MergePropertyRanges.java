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


class MergePropertyRanges extends ConversionWildcard<And> {
    /**
     * Create a wildcard node of type {@code t}.
     * Nodes of equal types require matched nodes to be equal.
     *
     * @param t type
     */
    MergePropertyRanges(int t) {
        super(t);
    }

    @Override
    public boolean isOk(And node) {
        return node.getLeft() instanceof PropertyRange && node.getRight() instanceof PropertyRange && ((PropertyRange) node.getLeft()).getPropertyName().equals(((PropertyRange) node.getRight()).getPropertyName());
    }

    @Override
    public NodeBase getClone() {
        return new MergePropertyRanges(type);
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (!(o instanceof MergePropertyRanges)) {
            throw new RuntimeException("Can't compare wildcard with " + o.getClass() + '.');
        }
        MergePropertyRanges wildcard = (MergePropertyRanges) o;
        return type == wildcard.type;
    }

    @Override
    public Class getClazz() {
        return And.class;
    }

    @Override
    public NodeBase convert(And matched) {
        return ((PropertyRange) matched.getLeft()).merge(((PropertyRange) matched.getRight()));
    }

    @Override
    public String getSimpleName() {
        return "mpr";
    }
}
