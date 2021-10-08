/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.lucene

import org.apache.lucene.store.BufferedChecksum
import org.apache.lucene.store.IndexOutput
import java.io.OutputStream
import java.util.zip.CRC32

internal class ExodusIndexOutput(directory: ExodusDirectory,
                                 name: String) : IndexOutput("ExodusIndexOutput[$name]", name) {

    private val output: OutputStream
    private var currentPosition = 0L
    private val crc = BufferedChecksum(CRC32())

    init {
        val vfs = directory.vfs
        val file = vfs.openFile(directory.environment.andCheckCurrentTransaction, name, true)
                ?: throw NullPointerException("Can't be")
        output = vfs.writeFile(directory.environment.andCheckCurrentTransaction, file)
    }

    override fun close() = output.close()

    override fun getFilePointer() = currentPosition

    override fun getChecksum() = crc.value

    override fun writeByte(b: Byte) {
        output.write(b.toInt())
        ++currentPosition
        crc.update(b.toInt())
    }

    override fun writeBytes(b: ByteArray, offset: Int, length: Int) {
        if (length > 0) {
            if (length == 1) {
                writeByte(b[offset])
            } else {
                output.write(b, offset, length)
                currentPosition += length.toLong()
                crc.update(b, offset, length)
            }
        }
    }
}
