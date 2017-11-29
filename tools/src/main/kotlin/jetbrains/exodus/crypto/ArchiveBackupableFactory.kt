/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.crypto

import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.Backupable
import jetbrains.exodus.backup.VirtualFileDescriptor
import mu.KLogging
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.util.*
import java.util.zip.GZIPInputStream

object ArchiveBackupableFactory : KLogging() {
    fun newBackupable(file: File, gzip: Boolean) = Backupable {
        val stream = if (gzip) {
            GZIPInputStream(FileInputStream(file))
        } else {
            FileInputStream(file)
        }
        val archive = ArchiveStreamFactory().createArchiveInputStream(BufferedInputStream(stream))
        object : BackupStrategy() {
            override fun listFiles() = object : MutableIterable<VirtualFileDescriptor> {
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
                    ArchiveEntryFileDescriptor(archive, it).also {
                        entry = null
                    }
                } ?: throw NoSuchElementException()
            }

            override fun remove() = throw UnsupportedOperationException()
        }
    }

    private class ArchiveEntryFileDescriptor(val archive: ArchiveInputStream,
                                             val entry: ArchiveEntry,
                                             val size: Long = entry.size) : VirtualFileDescriptor {
        override fun getPath() = entry.name // TODO

        override fun getName() = entry.name

        override fun hasContent() = !entry.isDirectory

        override fun getFileSize() = size

        override fun canBeEncrypted() = "version" != entry.name

        override fun getTimeStamp() = entry.lastModifiedDate.time

        override fun getInputStream() = archive

        override fun copy(acceptedSize: Long) = ArchiveEntryFileDescriptor(archive, entry, acceptedSize)
    }
}
