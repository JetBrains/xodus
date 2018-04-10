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
package jetbrains.exodus.log.replication

import jetbrains.exodus.util.IOUtil
import org.apache.commons.compress.archivers.zip.ZipFile
import java.io.File
import java.io.FileOutputStream

val preparedDB = File(NotEmptyLogReplicationTest::class.java.getResource("/logfiles.zip").toURI())

val preparedPaddedDB = File(NotEmptyLogReplicationTest::class.java.getResource("/logfiles-padded.zip").toURI())

fun File.unzipTo(restoreDir: File) {
    ZipFile(this).use { zipFile ->
        val zipEntries = zipFile.entries
        while (zipEntries.hasMoreElements()) {
            val zipEntry = zipEntries.nextElement()
            val entryFile = File(restoreDir, zipEntry.name)
            if (zipEntry.isDirectory) {
                entryFile.mkdirs()
            } else {
                entryFile.parentFile.mkdirs()
                FileOutputStream(entryFile).use { target -> zipFile.getInputStream(zipEntry).use { `in` -> IOUtil.copyStreams(`in`, target, IOUtil.BUFFER_ALLOCATOR) } }
            }
        }
    }
}
