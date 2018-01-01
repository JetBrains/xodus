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

import mu.KLogging
import org.apache.commons.compress.archivers.ArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import java.io.IOException

object ArchiveEncryptListenerFactory : KLogging() {
    fun newListener(archive: ArchiveOutputStream): EncryptListener {
        return when (archive) {
            is TarArchiveOutputStream -> {
                object : EncryptListener {
                    override fun onFile(header: FileHeader) {
                        logger.debug { "Start file $header" }
                        val entry = TarArchiveEntry(header.path + header.name)
                        entry.size = header.size
                        entry.setModTime(header.timestamp)
                        archive.putArchiveEntry(entry)
                    }

                    override fun onFileEnd(header: FileHeader) {
                        archive.closeArchiveEntry()
                    }

                    override fun onData(header: FileHeader, size: Int, data: ByteArray) {
                        archive.write(data, 0, size)
                    }
                }
            }
            is ZipArchiveOutputStream -> {
                object : EncryptListener {
                    override fun onFile(header: FileHeader) {
                        logger.debug { "Start file $header" }
                        val entry = ZipArchiveEntry(header.path + header.name)
                        entry.size = header.size
                        entry.time = header.timestamp
                        archive.putArchiveEntry(entry)
                    }

                    override fun onFileEnd(header: FileHeader) {
                        archive.closeArchiveEntry()
                    }

                    override fun onData(header: FileHeader, size: Int, data: ByteArray) {
                        archive.write(data, 0, size)
                    }
                }
            }
            else -> throw IOException("Unknown archive output stream")
        }
    }
}
