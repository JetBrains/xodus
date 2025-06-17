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
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinEntityIterableImpl;
import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.query.Utils.safe_equals;

public class PropertyEqual_G extends NodeBase {

    private final String name;
    @Nullable
    private final Comparable value;

    public PropertyEqual_G(String name, @Nullable Comparable value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public @Nullable Comparable getValue() {
        return value;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        if (value == null) {
            throw new UnsupportedOperationException("Implement this!!!");
        }
        return new GremlinEntityIterableImpl(
                queryEngine.getOStore().requireActiveTransaction(),
                new GremlinQuery.PropEqual(name, value)
        );
    }

    @Override
    public NodeBase getClone() {
        return new PropertyEqual_G(name, value);
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
        if (!(obj instanceof PropertyEqual_G)) {
            return false;
        }
        PropertyEqual_G propertyEqual = (PropertyEqual_G) obj;
        return safe_equals(name, propertyEqual.name) && safe_equals(value, propertyEqual.value);
    }

    @Override
    public String toString(String prefix) {
        return super.toString(prefix) + '(' + name + '=' + value + ") ";
    }

    @Override
    public StringBuilder getHandle(StringBuilder sb) {
        return super.getHandle(sb).append('(').append(name).append('=').append(value).append(')');
    }

    @Override
    public String getSimpleName() {
        return "pe";
    }
}
