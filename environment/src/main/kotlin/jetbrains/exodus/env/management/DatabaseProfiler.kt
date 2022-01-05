/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.env.management

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.executeInCommitLock
import jetbrains.exodus.management.MBeanBase

class DatabaseProfiler(private val env: EnvironmentImpl) : MBeanBase(getObjectName(env)), DatabaseProfilerMBean {

    private val txnProfiler = checkNotNull(env.txnProfiler)

    override fun reset() {
        env.executeInCommitLock {
            txnProfiler.reset()
        }
    }

    override fun dump() {
        env.executeInCommitLock {
            txnProfiler.dump()
        }
    }

    companion object {
        internal fun getObjectName(env: Environment) =
                "$PROFILER_OBJECT_NAME_PREFIX, location=${escapeLocation(env.location)}"
    }
}