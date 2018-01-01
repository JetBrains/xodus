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
package jetbrains.exodus.entitystore.util;

import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.util.SpinAllocator;

public class IntArrayListSpinAllocator {

    private static final int INITIAL_CAPACITY = 1024;
    private static final int MAXIMUM_KEEPING_CAPACITY = 8192;

    private IntArrayListSpinAllocator() {
    }

    private static class Creator implements SpinAllocator.ICreator<IntArrayList> {
        @Override
        public IntArrayList createInstance() {
            return new IntArrayList(INITIAL_CAPACITY);
        }
    }

    private static class Disposer implements SpinAllocator.IDisposer<IntArrayList> {
        @Override
        public void disposeInstance(final IntArrayList instance) {
            instance.clear();
            if (instance.getCapacity() > MAXIMUM_KEEPING_CAPACITY) {
                instance.setCapacity(INITIAL_CAPACITY);
            }
        }
    }

    private static final SpinAllocator<IntArrayList> allocator =
            new SpinAllocator<>(new Creator(), new Disposer());

    public static IntArrayList alloc() {
        return allocator.alloc();
    }

    public static void dispose(final IntArrayList instance) {
        allocator.dispose(instance);
    }
}
