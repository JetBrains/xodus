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
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

object DirectoryEncryptListenerFactory : KLogging() {

    fun newListener(folder: File): EncryptListener {
        return object : EncryptListener {
            var currentFolder = folder
            var fileOut: OutputStream? = null

            override fun onFile(header: FileHeader) {
                if (header.path.isNotEmpty()) {
                    currentFolder = File(folder, header.path)
                    currentFolder.mkdirs()
                } else {
                    currentFolder = folder
                }
                fileOut = FileOutputStream(File(currentFolder, header.name))
            }

            override fun onFileEnd(header: FileHeader) {
                fileOut?.close() ?: throw IllegalStateException("No file in progress")
            }

            override fun onData(header: FileHeader, size: Int, data: ByteArray) {
                fileOut?.write(data, 0, size) ?: throw IllegalStateException("No file in progress")
            }
        }
    }
}
