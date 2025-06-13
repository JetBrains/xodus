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
import jetbrains.exodus.entitystore.youtrackdb.iterate.property.YTDBPropertyCollectionContainsIterable;
import jetbrains.exodus.query.metadata.ModelMetaData;

import static jetbrains.exodus.query.Utils.safe_equals;

public class PropertyCollectionContains extends NodeBase {

    private final String name;
    private final Object value;

    public PropertyCollectionContains(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        var txn = queryEngine.getOStore().requireActiveTransaction();
        return new YTDBPropertyCollectionContainsIterable(txn, entityType, name, value);
    }

    @Override
    public NodeBase getClone() {
        return new PropertyCollectionContains(name, value);
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
        if (!(obj instanceof PropertyCollectionContains)) {
            return false;
        }
        PropertyCollectionContains right = (PropertyCollectionContains) obj;
        return safe_equals(name, right.name) && safe_equals(value, right.value);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + name + ' ' + value + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        return super.getHandle(sb).append('(').append(name).append(' ').append(value).append(')');
    }

    @Override
    public String getSimpleName() {
        return "pcc";
    }
}

