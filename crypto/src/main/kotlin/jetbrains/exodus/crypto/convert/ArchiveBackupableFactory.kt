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
package jetbrains.exodus.crypto.convert

import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.Backupable
import jetbrains.exodus.backup.VirtualFileDescriptor
import mu.KLogging
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.util.*
import java.util.zip.GZIPInputStream

object ArchiveBackupableFactory : KLogging() {
    private val separators = charArrayOf('\\', '/')

    fun newBackupable(stream: InputStream, gzip: Boolean) = Backupable {
        newArchiveBackupStrategy(ArchiveStreamFactory().createArchiveInputStream(BufferedInputStream(if (gzip) {
            GZIPInputStream(stream)
        } else {
            stream
        })))
    }

    fun newBackupable(file: File, gzip: Boolean) = newBackupable(file.inputStream(), gzip)

    private fun newArchiveBackupStrategy(archive: ArchiveInputStream): BackupStrategy {
        return object : BackupStrategy() {
            override fun getContents() = object : MutableIterable<VirtualFileDescriptor> {
                override fun iterator() = newArchiveIterator(archive)
            }

            override fun afterBackup() {
                archive.close()
            }
        }
    }

    private fun newArchiveIterator(archive: ArchiveInputStream): MutableIterator<VirtualFileDescriptor> {
        return object : MutableIterator<VirtualFileDescriptor> {
            var entry: ArchiveEntry? = null

            override fun hasNext(): Boolean {
                if (entry == null) {
                    entry = archive.nextEntry
                }
                return entry != null
            }

            override fun next(): VirtualFileDescriptor {
                if (entry == null) {
                    entry = archive.nextEntry
                }
                return entry?.let {
                    val entryName = it.name
                    val (path, name) = parseEntryPath(entryName)
                    ArchiveEntryFileDescriptor(archive, it, path, name).also {
                        entry = null
                    }
                } ?: throw NoSuchElementException()
            }

            override fun remove() = throw UnsupportedOperationException()
        }
    }

    private fun parseEntryPath(entryName: String): Pair<String, String> {
        val separatorIndex = entryName.lastIndexOfAny(separators)
        val path: String
        val name: String
        if (separatorIndex >= 0) {
            path = entryName.substring(0, separatorIndex + 1)
            name = entryName.substring(separatorIndex + 1, entryName.length)
        } else {
            path = ""
            name = entryName
        }
        return Pair(path, name)
    }

    private class ArchiveEntryFileDescriptor(val archive: ArchiveInputStream,
                                             val entry: ArchiveEntry,
                                             val _path: String,
                                             val _name: String,
                                             val size: Long = entry.size) : VirtualFileDescriptor {
        private val canBeEncrypted = "version" != _name && "xd.lck" != _name

        override fun getPath() = _path

        override fun getName() = _name

        override fun hasContent() = !entry.isDirectory

        override fun getFileSize() = size

        override fun canBeEncrypted() = canBeEncrypted

        override fun getTimeStamp() = entry.lastModifiedDate.time

        override fun getInputStream() = archive

        override fun shouldCloseStream() = false

        override fun copy(acceptedSize: Long) = ArchiveEntryFileDescriptor(archive, entry, _path, _name, acceptedSize)
    }
}
