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
package jetbrains.exodus.core.dataStructures.persistent

import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Ignore
import org.junit.Test
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

class PersistentObjectCacheStressTest {
    @Ignore
    @Test fun `collision test`() {
        val values = 50
        PersistentObjectCache<String, String>(values + 1).apply {
            (1..values).forEach {
                put("key$it", "value$it")
            }
            assertNotNull(get("key4"))
            assertNotNull(get("key12"))
            assertNotNull(get("key23"))
        }
    }

    @Ignore
    @Test fun `cache stress`() {
        val values = 50
        PersistentObjectCache<String, String>(values + 1).apply {
            (1..values).forEach {
                put("key$it", "value$it")
            }
            val cached = arrayListOf<String>()
            forEachEntry { key, value ->
                cached.add(key)
            }

            val sema = Semaphore(0)
            val count = 10
            val wasError = AtomicBoolean()
            val threads = (1..count).map {
                Thread({
                    var iteration = 0
                    sema.acquire()
                    try {
                        while (true) {
                            iteration++
                            put("key4", "value4")
                            cached.forEach {
                                assertNotNull(get(it))
                            }
                        }
                    } catch (t: Throwable) {
                        println("Error at iteration: $iteration , cache size: ${count()}")
                        t.printStackTrace()
                        wasError.set(true)
                    }
                }, "Runner $it")
            }
            threads.forEach(Thread::start)
            sema.release(count)
            threads.forEach(Thread::join)
            assertFalse(wasError.get())
        }
    }
}
