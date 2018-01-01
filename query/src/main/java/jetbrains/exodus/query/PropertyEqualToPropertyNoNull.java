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


/**
 * {@code to} field of the matched {@code LinkEqual} must be null
 */
class PropertyEqualToPropertyNoNull extends ConversionWildcard<PropertyEqual> {
    /**
     * Create a wildcard node of type {@code t}.
     * Nodes of equal types require matched nodes to be equal.
     *
     * @param t type
     */
    PropertyEqualToPropertyNoNull(int t) {
        super(t);
    }

    @Override
    public boolean isOk(PropertyEqual node) {
        return node.getValue() == null;
    }

    @Override
    public NodeBase getClone() {
        return new PropertyEqualToPropertyNoNull(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PropertyEqualToPropertyNoNull)) {
            throw new RuntimeException("Can't compare wildcard with " + obj.getClass() + '.');
        }
        PropertyEqualToPropertyNoNull wildcard = (PropertyEqualToPropertyNoNull) obj;
        return type == wildcard.type;
    }

    @Override
    public Class getClazz() {
        return PropertyEqual.class;
    }

    @Override
    public NodeBase convert(PropertyEqual matched) {
        return new PropertyNotNull(matched.getName());
    }

    @Override
    public String getSimpleName() {
        return "pe2pnn";
    }
}
