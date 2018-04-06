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
package jetbrains.exodus.env.replication

import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.MetaTreePrototype
import jetbrains.exodus.env.executeInCommitLock
import jetbrains.exodus.env.reopenMetaTree
import jetbrains.exodus.log.replication.FileFactory
import jetbrains.exodus.log.replication.LogAppender

object EnvironmentAppender {

    @JvmStatic
    fun appendEnvironment(env: EnvironmentImpl, delta: EnvironmentReplicationDelta, fileFactory: FileFactory) {
        env.executeInCommitLock {
            val logTip = env.log.beginWrite()
            val confirm = LogAppender.appendLog(env.log, delta, fileFactory, logTip)

            env.reopenMetaTree(object : MetaTreePrototype {
                override fun treeAddress() = delta.metaTreeAddress

                override fun rootAddress() = delta.rootAddress
            }, logTip, confirm)
        }
    }
}
