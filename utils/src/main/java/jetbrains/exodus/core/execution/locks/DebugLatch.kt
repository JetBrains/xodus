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

import org.slf4j.LoggerFactory
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*

internal class DebugLatch : ReleaseLatch() {
    private var acquireTime: Date? = null
    private var acquireTrace: Array<StackTraceElement>? = null
    @Synchronized
    @Throws(InterruptedException::class)
    override fun acquire() {
        while (owner != null) {
            logOwner()
            wait()
        }
        owner = Thread.currentThread()
        gatherOwnerInfo()
    }

    @Synchronized
    @Throws(InterruptedException::class)
    override fun acquire(timeout: Long): Boolean {
        if (owner != null) {
            logOwner()
            wait(timeout)
            if (owner != null) {
                return false
            }
        }
        owner = Thread.currentThread()
        gatherOwnerInfo()
        return true
    }

    @Synchronized
    override fun tryAcquire(): Boolean {
        if (owner != null) {
            logOwner()
            return false
        }
        owner = Thread.currentThread()
        gatherOwnerInfo()
        return true
    }

    @Synchronized
    override fun release() {
        clearOwnerInfo()
        owner = null
        notify()
    }

    private fun logOwner() {
        try {
            log.debug("-------------------------------------------------------------")
            log.debug("DebugLatch is closed at [" + df.format(acquireTime) + "]. Will wait for latch owner [" + owner!!.name + ']')
            log.debug("Owner stack trace:")
            for (e in acquireTrace!!) {
                log.debug(e.toString())
            }
        } catch (e: Throwable) {
            // NPE is expected if we are tring to log owner when owner is about to release the latch
        } finally {
            log.debug("-------------------------------------------------------------")
        }
    }

    private fun gatherOwnerInfo() {
        acquireTime = Date()
        acquireTrace = Throwable().stackTrace
    }

    private fun clearOwnerInfo() {
        acquireTime = null
        acquireTrace = null
    }

    companion object {
        private val log = LoggerFactory.getLogger(Latch::class.java)
        private val df: DateFormat = SimpleDateFormat("dd MMM yyyy kk:mm:ss,SSS")
    }
}
