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
package jetbrains.exodus.core.execution.locks;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

class DebugLatch extends ReleaseLatch {

    private static final Logger log = LoggerFactory.getLogger(Latch.class);
    private static final DateFormat df = new SimpleDateFormat("dd MMM yyyy kk:mm:ss,SSS");

    private Date acquireTime = null;
    private StackTraceElement[] acquireTrace = null;

    @Override
    public synchronized void acquire() throws InterruptedException {
        while (owner != null) {
            logOwner();
            wait();
        }
        owner = Thread.currentThread();
        gatherOwnerInfo();
    }

    @Override
    public synchronized boolean acquire(long timeout) throws InterruptedException {
        if (owner != null) {
            logOwner();
            wait(timeout);
            if (owner != null) {
                return false;
            }
        }
        owner = Thread.currentThread();
        gatherOwnerInfo();
        return true;
    }

    @Override
    public synchronized boolean tryAcquire() {
        if (owner != null) {
            logOwner();
            return false;
        }
        owner = Thread.currentThread();
        gatherOwnerInfo();
        return true;
    }

    @Override
    public synchronized void release() {
        clearOwnerInfo();
        owner = null;
        notify();
    }

    private void logOwner() {
        try {
            log.debug("-------------------------------------------------------------");
            log.debug("DebugLatch is closed at [" + df.format(acquireTime) + "]. Will wait for latch owner [" + owner.getName() + ']');
            log.debug("Owner stack trace:");
            for (StackTraceElement e : acquireTrace) {
                log.debug(e.toString());
            }
        } catch (Throwable e) {
            // NPE is expected if we are tring to log owner when owner is about to release the latch
        } finally {
            log.debug("-------------------------------------------------------------");
        }
    }

    private void gatherOwnerInfo() {
        acquireTime = new Date();
        acquireTrace = new Throwable().getStackTrace();
    }

    private void clearOwnerInfo() {
        acquireTime = null;
        acquireTrace = null;
    }

}
