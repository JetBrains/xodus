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
package jetbrains.exodus.log

import jetbrains.exodus.ExodusException
import java.nio.ByteBuffer

@Suppress("unused")
open class BackupMetadata protected constructor(
    useFirstFile: Boolean, rootAddress: Long,
    isCorrectlyClosed: Boolean, pageSize: Int, currentVersion: Long,
    environmentFormatVersion: Int, fileLengthBoundary: Long,
    val lastFileAddress: Long, val lastFileOffset: Long
) : StartupMetadata(
    useFirstFile, rootAddress, isCorrectlyClosed, pageSize, currentVersion,
    environmentFormatVersion, fileLengthBoundary
) {

    companion object {
        const val BACKUP_METADATA_FILE_NAME = "backup-metadata"
        private const val LAST_FILE_ADDRESS: Int = StartupMetadata.FILE_SIZE
        private const val LAST_FILE_OFFSET = LAST_FILE_ADDRESS + java.lang.Long.BYTES
        const val FILE_SIZE = this.LAST_FILE_OFFSET + java.lang.Long.BYTES
        fun serialize(
            version: Long, environmentFormatVersion: Int,
            rootAddress: Long, pageSize: Int,
            fileLengthBoundary: Long,
            correctlyClosedFlag: Boolean, lastFileAddress: Long,
            lastFileOffset: Long
        ): ByteBuffer {
            val content = ByteBuffer.allocate(FILE_SIZE)
            content.putLong(FILE_VERSION_OFFSET, version)
            content.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION)
            content.putInt(ENVIRONMENT_FORMAT_VERSION_OFFSET, environmentFormatVersion)
            content.putLong(DB_ROOT_ADDRESS_OFFSET, rootAddress)
            content.putInt(PAGE_SIZE_OFFSET, pageSize)
            content.putLong(FILE_LENGTH_BOUNDARY_OFFSET, fileLengthBoundary)
            content.put(
                CORRECTLY_CLOSED_FLAG_OFFSET,
                if (correctlyClosedFlag) 1.toByte() else 0
            )
            content.putLong(LAST_FILE_ADDRESS, lastFileAddress)
            content.putLong(LAST_FILE_OFFSET, lastFileOffset)
            val hash: Long = BufferedDataWriter.xxHash.hash(
                content,
                FILE_VERSION_OFFSET,
                FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED
            )
            content.putLong(HASHCODE_OFFSET, hash)
            return content
        }

        fun deserialize(content: ByteBuffer, version: Long, useFirstFile: Boolean): BackupMetadata? {
            val formatVersion = content.getInt(FORMAT_VERSION_OFFSET)
            if (formatVersion != FORMAT_VERSION) {
                throw ExodusException(
                    "Invalid format of startup metadata. { expected : " + FORMAT_VERSION +
                            ", actual: " + formatVersion + "}"
                )
            }
            val environmentFormatVersion = content.getInt(ENVIRONMENT_FORMAT_VERSION_OFFSET)
            val dbRootAddress = content.getLong(DB_ROOT_ADDRESS_OFFSET)
            val pageSize = content.getInt(PAGE_SIZE_OFFSET)
            val fileLengthBoundary = content.getLong(FILE_LENGTH_BOUNDARY_OFFSET)
            val closedFlag: Boolean = content.get(CORRECTLY_CLOSED_FLAG_OFFSET) > 0
            val lastFileAddress = content.getLong(LAST_FILE_ADDRESS)
            val lastFileOffset = content.getLong(LAST_FILE_OFFSET)
            val hash: Long = BufferedDataWriter.xxHash.hash(
                content,
                FILE_VERSION_OFFSET,
                FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED
            )
            return if (hash != content.getLong(HASHCODE_OFFSET)) {
                null
            } else BackupMetadata(
                useFirstFile, dbRootAddress, closedFlag, pageSize, version,
                environmentFormatVersion, fileLengthBoundary, lastFileAddress, lastFileOffset
            )
        }
    }
}
