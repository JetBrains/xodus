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
package jetbrains.exodus.core.execution.locks

internal open class ReleaseLatch : Latch() {
    protected var owner: Thread? = null

    @get:Synchronized
    override val ownerName: String
        get() = if (owner == null) "no owner" else owner!!.name

    @Synchronized
    @Throws(InterruptedException::class)
    override fun acquire() {
        while (owner != null) {
            wait()
        }
        owner = Thread.currentThread()
    }

    @Synchronized
    @Throws(InterruptedException::class)
    override fun acquire(timeout: Long): Boolean {
        if (owner != null) {
            wait(timeout)
            if (owner != null) {
                return false
            }
        }
        owner = Thread.currentThread()
        return true
    }

    @Synchronized
    override fun tryAcquire(): Boolean {
        if (owner != null) {
            return false
        }
        owner = Thread.currentThread()
        return true
    }

    @Synchronized
    override fun release() {
        owner = null
        notify()
    }
}
