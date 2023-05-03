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
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.util.IOUtil.readFully
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

open class StartupMetadata protected constructor(
    val isUseFirstFile: Boolean, @field:Volatile var rootAddress: Long,
    val isCorrectlyClosed: Boolean, val pageSize: Int, val currentVersion: Long,
    val environmentFormatVersion: Int,
    val fileLengthBoundary: Long
) {

    @Throws(IOException::class)
    fun closeAndUpdate(reader: FileDataReader) {
        val dbPath = Paths.get(reader.location)
        val content = serialize(
            currentVersion, environmentFormatVersion, rootAddress,
            pageSize, fileLengthBoundary, true
        )
        store(content, dbPath, isUseFirstFile)
    }

    companion object {
        const val HASHCODE_OFFSET = 0
        const val HASH_CODE_SIZE = java.lang.Long.BYTES
        const val FILE_VERSION_OFFSET = HASHCODE_OFFSET + HASH_CODE_SIZE
        private const val FILE_VERSION_BYTES = java.lang.Long.BYTES
        const val FORMAT_VERSION_OFFSET = FILE_VERSION_OFFSET + FILE_VERSION_BYTES
        private const val FORMAT_VERSION_BYTES = Integer.BYTES
        const val ENVIRONMENT_FORMAT_VERSION_OFFSET = FORMAT_VERSION_OFFSET + FORMAT_VERSION_BYTES
        private const val ENVIRONMENT_FORMAT_VERSION_BYTES = Integer.BYTES
        const val DB_ROOT_ADDRESS_OFFSET = ENVIRONMENT_FORMAT_VERSION_OFFSET + ENVIRONMENT_FORMAT_VERSION_BYTES
        private const val DB_ROOT_BYTES = java.lang.Long.BYTES
        const val PAGE_SIZE_OFFSET = DB_ROOT_ADDRESS_OFFSET + DB_ROOT_BYTES
        private const val PAGE_SIZE_BYTES = Integer.BYTES
        const val FILE_LENGTH_BOUNDARY_OFFSET = PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES
        private const val FILE_LENGTH_BOUNDARY_BYTES = java.lang.Long.BYTES
        const val CORRECTLY_CLOSED_FLAG_OFFSET = FILE_LENGTH_BOUNDARY_OFFSET + FILE_LENGTH_BOUNDARY_BYTES
        private const val CLOSED_FLAG_BYTES = java.lang.Byte.BYTES
        const val FILE_SIZE = CORRECTLY_CLOSED_FLAG_OFFSET + CLOSED_FLAG_BYTES
        const val FIRST_FILE_NAME = "startup-metadata-0"
        const val SECOND_FILE_NAME = "startup-metadata-1"
        const val FORMAT_VERSION = 1

        @Throws(IOException::class)
        fun open(
            reader: FileDataReader,
            isReadOnly: Boolean, pageSize: Int,
            environmentFormatVersion: Int,
            fileLengthBoundary: Long,
            logContainsBlocks: Boolean
        ): StartupMetadata? {
            val dbPath = Paths.get(reader.location)
            val firstFilePath = dbPath.resolve(FIRST_FILE_NAME)
            val secondFilePath = dbPath.resolve(SECOND_FILE_NAME)
            val firstFileVersion: Long
            val secondFileVersion: Long
            val firstFileContent: ByteBuffer?
            val secondFileContent: ByteBuffer?
            val firstFileExist = Files.exists(firstFilePath)
            if (firstFileExist) {
                FileChannel.open(firstFilePath, StandardOpenOption.READ)
                    .use { channel -> firstFileContent = readFully(channel) }
                firstFileVersion = getFileVersion(firstFileContent)
            } else {
                firstFileVersion = -1
                firstFileContent = null
            }
            val secondFileExist = Files.exists(secondFilePath)
            if (secondFileExist) {
                FileChannel.open(secondFilePath, StandardOpenOption.READ)
                    .use { channel -> secondFileContent = readFully(channel) }
                secondFileVersion = getFileVersion(secondFileContent)
            } else {
                secondFileVersion = -1
                secondFileContent = null
            }
            if (firstFileVersion < 0 && firstFileExist && !isReadOnly) {
                Files.deleteIfExists(firstFilePath)
            }
            if (secondFileVersion < 0 && secondFileExist && !isReadOnly) {
                Files.deleteIfExists(secondFilePath)
            }
            val content: ByteBuffer?
            val nextVersion: Long
            val useFirstFile: Boolean
            if (firstFileVersion < secondFileVersion) {
                if (firstFileExist && !isReadOnly) {
                    Files.deleteIfExists(firstFilePath)
                }
                nextVersion = secondFileVersion + 1
                content = secondFileContent
                useFirstFile = true
            } else if (secondFileVersion < firstFileVersion) {
                if (secondFileExist && !isReadOnly) {
                    Files.deleteIfExists(secondFilePath)
                }
                nextVersion = firstFileVersion + 1
                content = firstFileContent
                useFirstFile = false
            } else {
                content = null
                nextVersion = 0
                useFirstFile = true
            }
            if (content == null) {
                if (!logContainsBlocks) {
                    val updatedMetadata = serialize(
                        1, environmentFormatVersion, -1,
                        pageSize, fileLengthBoundary, false
                    )
                    store(updatedMetadata, dbPath, useFirstFile)
                }
                return null
            }
            val result = deserialize(content, nextVersion + 1, !useFirstFile)
            if (!isReadOnly) {
                val updatedMetadata = serialize(
                    nextVersion, result.environmentFormatVersion, -1,
                    result.pageSize, result.fileLengthBoundary, false
                )
                store(updatedMetadata, dbPath, useFirstFile)
            }
            return result
        }

        fun createStub(
            pageSize: Int, isCorrectlyClosed: Boolean,
            environmentFormatVersion: Int, fileLengthBoundary: Long
        ): StartupMetadata {
            return StartupMetadata(
                false, -1, isCorrectlyClosed,
                pageSize, 1,
                environmentFormatVersion, fileLengthBoundary
            )
        }

        @Throws(IOException::class)
        private fun store(content: ByteBuffer, dbPath: Path, useFirstFile: Boolean) {
            val filePath: Path = if (useFirstFile) {
                dbPath.resolve(FIRST_FILE_NAME)
            } else {
                dbPath.resolve(SECOND_FILE_NAME)
            }
            FileChannel.open(
                filePath, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
            ).use { channel ->
                while (content.remaining() > 0) {
                    channel.write(content)
                }
                channel.force(true)
            }
            if (useFirstFile) {
                Files.deleteIfExists(dbPath.resolve(SECOND_FILE_NAME))
            } else {
                Files.deleteIfExists(dbPath.resolve(FIRST_FILE_NAME))
            }
        }


        @JvmStatic
        fun getFileVersion(content: ByteBuffer?): Long {
            if (content!!.remaining() != FILE_SIZE) {
                return -1
            }
            val hash: Long = BufferedDataWriter.xxHash.hash(
                content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED
            )
            return if (hash != content.getLong(HASHCODE_OFFSET)) {
                -1
            } else content.getInt(FORMAT_VERSION_OFFSET).toLong()
        }


        fun serialize(
            version: Long, environmentFormatVersion: Int,
            rootAddress: Long, pageSize: Int,
            fileLengthBoundary: Long,
            correctlyClosedFlag: Boolean
        ): ByteBuffer {
            val content = ByteBuffer.allocate(FILE_SIZE)
            content.putLong(FILE_VERSION_OFFSET, version)
            content.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION)
            content.putInt(ENVIRONMENT_FORMAT_VERSION_OFFSET, environmentFormatVersion)
            content.putLong(DB_ROOT_ADDRESS_OFFSET, rootAddress)
            content.putInt(PAGE_SIZE_OFFSET, pageSize)
            content.putLong(FILE_LENGTH_BOUNDARY_OFFSET, fileLengthBoundary)
            content.put(CORRECTLY_CLOSED_FLAG_OFFSET, if (correctlyClosedFlag) 1.toByte() else 0)
            val hash: Long = BufferedDataWriter.xxHash.hash(
                content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED
            )
            content.putLong(HASHCODE_OFFSET, hash)
            return content
        }

        @JvmStatic
        fun deserialize(content: ByteBuffer, version: Long, useFirstFile: Boolean): StartupMetadata {
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
            val closedFlag = content[CORRECTLY_CLOSED_FLAG_OFFSET] > 0
            return StartupMetadata(
                useFirstFile, dbRootAddress, closedFlag, pageSize, version,
                environmentFormatVersion,
                fileLengthBoundary
            )
        }

        @JvmStatic
        fun isStartupFileName(name: String): Boolean {
            return FIRST_FILE_NAME == name || SECOND_FILE_NAME == name
        }
    }
}

