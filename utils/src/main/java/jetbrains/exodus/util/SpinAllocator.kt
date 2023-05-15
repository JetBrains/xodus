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

import java.util.concurrent.atomic.AtomicBoolean

/**
 * SpinAllocator can be used for allocating short living objects of type T.
 * Avoiding reentering allocations, MAXIMUM_POOLED_ALLOCATIONS are concurrently possible.
 * If more allocations are required, allocator returns non-pooled newly created objects.
 */
open class SpinAllocator<T>(
    private val creator: ICreator<T>,
    private val disposer: IDisposer<T>?,
    maxAllocations: Int
) {
    interface ICreator<T> {
        fun createInstance(): T
    }

    interface IDisposer<T> {
        fun disposeInstance(instance: T)
    }

    private val employed: Array<AtomicBoolean?>
    private val objects: Array<T?>

    constructor(creator: ICreator<T>, disposer: IDisposer<T>?) : this(creator, disposer, MAXIMUM_ALLOCATIONS)

    init {
        employed = arrayOfNulls(maxAllocations)
        objects = arrayOfNulls<Any>(maxAllocations) as Array<T?>
        for (i in 0 until maxAllocations) {
            employed[i] = AtomicBoolean(false)
        }
    }

    fun alloc(): T? {
        for (i in 0 until MAXIMUM_ALLOCATIONS) {
            if (!employed[i]!!.getAndSet(true)) {
                var result = objects[i]
                if (result == null) {
                    result = creator.createInstance()
                    objects[i] = result
                }
                return result
            }
        }
        return creator.createInstance()
    }

    fun dispose(instance: T): Boolean {
        for (i in 0 until MAXIMUM_ALLOCATIONS) {
            if (objects[i] === instance) {
                if (!employed[i]!!.get()) {
                    throw RuntimeException("Instance is already disposed.")
                }
                disposer?.disposeInstance(instance)
                employed[i]!!.set(false)
                return true
            }
        }
        // allocation wasn't pooled
        return false
    }

    companion object {
        private const val MAXIMUM_ALLOCATIONS = 50
    }
}
