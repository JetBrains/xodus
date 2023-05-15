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
package jetbrains.exodus.util

import jetbrains.exodus.util.SpinAllocator.ICreator
import jetbrains.exodus.util.SpinAllocator.IDisposer

/**
 * StringBuilderSpinAllocator reuses StringBuilder instances performing non-blocking allocation and dispose.
 */
object StringBuilderSpinAllocator {
    private val allocator = SpinAllocator(Creator(), Disposer(), 100)
    @JvmStatic
    fun alloc(): StringBuilder? {
        return allocator.alloc()
    }

    fun dispose(instance: StringBuilder?) {
        allocator.dispose(instance!!)
    }

    private class Creator : ICreator<StringBuilder> {
        override fun createInstance(): StringBuilder {
            return StringBuilder()
        }
    }

    private class Disposer : IDisposer<StringBuilder> {
        override fun disposeInstance(instance: StringBuilder) {
            instance.setLength(0)
            if (instance.capacity() > 4096) {
                instance.trimToSize()
            }
        }
    }
}
