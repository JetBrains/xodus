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

class ReleaseLatch extends Latch {

    protected Thread owner = null;

    @Override
    public synchronized String getOwnerName() {
        return owner == null ? "no owner" : owner.getName();
    }

    @Override
    public synchronized void acquire() throws InterruptedException {
        while (owner != null) {
            wait();
        }
        owner = Thread.currentThread();
    }

    @Override
    public synchronized boolean acquire(long timeout) throws InterruptedException {
        if (owner != null) {
            wait(timeout);
            if (owner != null) {
                return false;
            }
        }
        owner = Thread.currentThread();
        return true;
    }

    @Override
    public synchronized boolean tryAcquire() {
        if (owner != null) {
            return false;
        }
        owner = Thread.currentThread();
        return true;
    }

    @Override
    public synchronized void release() {
        owner = null;
        notify();
    }

}
