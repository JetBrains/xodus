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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterator;
import org.jetbrains.annotations.NotNull;

public final class EntityTypeFilteredIterator extends NonDisposableEntityIterator {

    private final EntityIterator source;
    private final int entityTypeId;
    private boolean hasNext;
    private boolean hasNextValid;
    private EntityId nextId;

    public EntityTypeFilteredIterator(@NotNull final EntityIterableBase source,
                                      final int entityTypeId) {
        super(source);
        this.source = source.iterator();
        this.entityTypeId = entityTypeId;
    }

    @Override
    protected boolean hasNextImpl() {
        checkHasNext();
        return hasNext;
    }

    @Override
    protected EntityId nextIdImpl() {
        checkHasNext();
        hasNextValid = false;
        return nextId;
    }

    private void checkHasNext() {
        if (!hasNextValid) {
            hasNextValid = true;
            nextId = null;
            while (source.hasNext()) {
                final EntityId nextId = source.nextId();
                if (nextId == null || entityTypeId == nextId.getTypeId()) {
                    this.nextId = nextId;
                    hasNext = true;
                    return;
                }
            }
            hasNext = false;
        }
    }
}
