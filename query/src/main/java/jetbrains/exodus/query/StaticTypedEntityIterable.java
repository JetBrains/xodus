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

import java.util.Iterator;

public abstract class StaticTypedEntityIterable implements Iterable<Entity> {
    protected String entityType; // TODO: make final?
    protected final QueryEngine queryEngine;

    protected StaticTypedEntityIterable(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public String getEntityType() {
        return entityType;
    }

    public abstract Iterable<Entity> instantiate();

    @Override
    public Iterator<Entity> iterator() {
        return instantiate().iterator();
    }

    public static Iterable<Entity> instantiate(Iterable<Entity> it) {
        return it instanceof StaticTypedEntityIterable ? ((StaticTypedEntityIterable) it).instantiate() : it;
    }
}
