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

import java.util.*

data class ReplicationDelta(
        override val startAddress: Long,
        override val highAddress: Long,
        override val fileSize: Long,
        val metaTreeAddress: Long,
        val rootAddress: Long,
        override val files: LongArray
) : EnvironmentReplicationDelta {
    override fun treeAddress() = metaTreeAddress

    override fun rootAddress() = rootAddress

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ReplicationDelta) return false

        if (startAddress != other.startAddress) return false
        if (highAddress != other.highAddress) return false
        if (fileSize != other.fileSize) return false
        if (metaTreeAddress != other.metaTreeAddress) return false
        if (rootAddress != other.rootAddress) return false
        if (!Arrays.equals(files, other.files)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = startAddress.hashCode()
        result = 31 * result + highAddress.hashCode()
        result = 31 * result + fileSize.hashCode()
        result = 31 * result + metaTreeAddress.hashCode()
        result = 31 * result + rootAddress.hashCode()
        result = 31 * result + Arrays.hashCode(files)
        return result
    }
}
