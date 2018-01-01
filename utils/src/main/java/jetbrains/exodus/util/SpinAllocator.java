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
package jetbrains.exodus.util;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SpinAllocator can be used for allocating short living objects of type T.
 * Avoiding reentering allocations, MAXIMUM_POOLED_ALLOCATIONS are concurrently possible.
 * If more allocations are required, allocator returns non-pooled newly created objects.
 */
public class SpinAllocator<T> {

    private static final int MAXIMUM_ALLOCATIONS = 50;

    public interface ICreator<T> {
        T createInstance();
    }

    public interface IDisposer<T> {
        void disposeInstance(T instance);
    }

    private final AtomicBoolean[] employed;
    private final T[] objects;
    private final ICreator<T> creator;
    private final IDisposer<T> disposer;

    public SpinAllocator(final ICreator<T> creator, @Nullable final IDisposer<T> disposer) {
        this(creator, disposer, MAXIMUM_ALLOCATIONS);
    }

    @SuppressWarnings({"unchecked"})
    public SpinAllocator(final ICreator<T> creator, final IDisposer<T> disposer, int maxAllocations) {
        this.creator = creator;
        this.disposer = disposer;
        employed = new AtomicBoolean[maxAllocations];
        objects = (T[]) new Object[maxAllocations];
        for (int i = 0; i < maxAllocations; ++i) {
            employed[i] = new AtomicBoolean(false);
        }
    }

    public T alloc() {
        for (int i = 0; i < MAXIMUM_ALLOCATIONS; ++i) {
            if (!employed[i].getAndSet(true)) {
                T result = objects[i];
                if (result == null) {
                    objects[i] = result = creator.createInstance();
                }
                return result;
            }
        }
        return creator.createInstance();
    }

    public void dispose(final T instance) {
        for (int i = 0; i < MAXIMUM_ALLOCATIONS; ++i) {
            if (objects[i] == instance) {
                if (!employed[i].get()) {
                    throw new RuntimeException("Instance is already disposed.");
                }
                if (disposer != null) {
                    disposer.disposeInstance(instance);
                }
                employed[i].set(false);
                return;
            }
        }
        // do nothing, allocation wasn't pooled
    }
}
