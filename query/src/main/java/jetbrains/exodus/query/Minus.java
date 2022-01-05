/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
import jetbrains.exodus.query.metadata.ModelMetaData;

public class Minus extends BinaryOperator {
    public Minus(final NodeBase left, final NodeBase right) {
        super(left, right);
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        return queryEngine.excludeAdjusted(
            getLeft().instantiate(entityType, queryEngine, metaData, context),
            getRight().instantiate(entityType, queryEngine, metaData, context));
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
        return obj instanceof Minus && super.equals(obj);
    }

    @Override
    public NodeBase getClone() {
        return new Minus(getLeft().getClone(), getRight().getClone());
    }

    @Override
    public String getSimpleName() {
        return "mns";
    }
}
