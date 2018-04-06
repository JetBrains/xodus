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
package jetbrains.exodus.entitystore.replication

import jetbrains.exodus.env.Environment
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class GcTransaction(val env: Environment) {
    companion object {
        private val idSequence = AtomicLong()
    }

    private val thread = AtomicReference<MyThread>()
    val id = idSequence.incrementAndGet()

    fun start(): Boolean {
        if (thread.get() == null) {
            val t = MyThread(env, Semaphore(0))
            if (thread.compareAndSet(null, t)) {
                t.start()
                return true
            }
        }
        return false
    }

    fun stop(): Boolean {
        val t = thread.get()
        if (t != null) {
            if (thread.compareAndSet(t, null)) {
                t.sema.release()
                return true
            }
        }
        return false
    }

    private inner class MyThread(environment: Environment, val sema: Semaphore) : Thread(Runnable {
        val txn = environment.beginTransaction()
        try {
            sema.acquire()
        } catch (ie: InterruptedException) {
            Thread.currentThread().interrupt()
            ie.printStackTrace()
        } finally {
            txn.abort()
        }
    }, "Thread with transaction preventing files from being deleted #$id")
}
