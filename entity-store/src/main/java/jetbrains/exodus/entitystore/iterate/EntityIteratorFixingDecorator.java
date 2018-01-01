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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Some EntityIteratorBase instances to dot allow to call hasNext() several times successively
 * without calling nextId(). This decorator fixes decorated entity iterator.
 */
public class EntityIteratorFixingDecorator extends EntityIteratorBase {

    private final EntityIteratorBase iterator;
    private boolean hasNext;
    private boolean hasNextValid;

    public EntityIteratorFixingDecorator(@NotNull final EntityIterableBase iterable,
                                         @NotNull final EntityIteratorBase iterator) {
        super(iterable);
        this.iterator = iterator;
    }

    @Override
    protected boolean hasNextImpl() {
        if (!hasNextValid) {
            hasNext = iterator.hasNextImpl();
            hasNextValid = true;
        }
        return hasNext;
    }

    @Override
    @Nullable
    public EntityId nextIdImpl() {
        final EntityId result = hasNextImpl() ? iterator.nextIdImpl() : null;
        hasNextValid = false;
        return result;
    }

    @Override
    public boolean shouldBeDisposed() {
        return iterator.shouldBeDisposed();
    }

    @Override
    public boolean dispose() {
        return iterator.dispose();
    }
}
