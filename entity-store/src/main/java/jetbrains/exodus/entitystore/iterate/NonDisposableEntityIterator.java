/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.EntityStoreException;
import org.jetbrains.annotations.NotNull;

/*
 * Non-disposable iterator doesn't actually mean that it cannot be disposed. It can but it shouldn't be certainly.
 */
public abstract class NonDisposableEntityIterator extends EntityIteratorBase {

    protected NonDisposableEntityIterator(@NotNull final EntityIterableBase iterable) {
        super(iterable);
    }

    @Override
    public final boolean shouldBeDisposed() {
        return false;
    }

    @Override
    public final boolean dispose() {
        throw new EntityStoreException("Attempt to dispose non-disposable iterator");
    }
}
