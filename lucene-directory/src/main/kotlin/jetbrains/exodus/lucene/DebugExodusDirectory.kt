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
package jetbrains.exodus.lucene

import jetbrains.exodus.env.ContextualEnvironment
import jetbrains.exodus.env.StoreConfig
import org.apache.lucene.index.IndexFileNames
import org.apache.lucene.store.*
import java.io.IOException

class DebugExodusDirectory : Directory {

    private val directory: ExodusDirectory
    private val debugDirectory: RAMDirectory

    @JvmOverloads constructor(env: ContextualEnvironment,
                              contentsStoreConfig: StoreConfig = StoreConfig.WITH_DUPLICATES,
                              directoryConfig: ExodusDirectoryConfig = ExodusDirectoryConfig()) {
        directory = ExodusDirectory(env, contentsStoreConfig, directoryConfig)
        debugDirectory = RAMDirectory()
    }

    override fun listAll(): Array<String> = debugDirectory.listAll()

    @Throws(IOException::class)
    override fun deleteFile(name: String) {
        directory.deleteFile(name)
        debugDirectory.deleteFile(name)
    }

    @Throws(IOException::class)
    override fun fileLength(name: String): Long {
        val result = directory.fileLength(name)
        if (result != debugDirectory.fileLength(name)) {
            throwDebugMismatch()
        }
        return result
    }

    @Throws(IOException::class)
    override fun createOutput(name: String, context: IOContext): IndexOutput {
        return DebugIndexOutput(name, context)
    }

    @Throws(IOException::class)
    override fun createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput {
        return createOutput(IndexFileNames.segmentFileName(prefix, suffix + '_' + directory.nextTicks(), "tmp"), context)
    }

    @Throws(IOException::class)
    override fun sync(names: Collection<String>) {
        directory.sync(names)
        debugDirectory.sync(names)
    }

    @Throws(IOException::class)
    override fun rename(source: String, dest: String) {
        directory.rename(source, dest)
        debugDirectory.rename(source, dest)
    }

    @Throws(IOException::class)
    override fun syncMetaData() {
        directory.syncMetaData()
        debugDirectory.syncMetaData()
    }

    @Throws(IOException::class)
    override fun openInput(name: String, context: IOContext): IndexInput {
        return DebugIndexInput(name, context)
    }

    @Throws(IOException::class)
    override fun obtainLock(name: String): Lock {
        return debugDirectory.obtainLock(name)
    }

    override fun close() {
        directory.close()
        debugDirectory.close()
    }

    private fun throwDebugMismatch() {
        throw RuntimeException("Debug directory mismatch")
    }

    private inner class DebugIndexOutput(name: String, context: IOContext)
        : IndexOutput("DebugIndexOutput[$name]", name) {

        private val output: IndexOutput = directory.createOutput(name, context)
        private val debugOutput: IndexOutput = debugDirectory.createOutput(name, context)

        @Throws(IOException::class)
        override fun close() {
            output.close()
            debugOutput.close()
        }

        override fun getFilePointer(): Long {
            val result = output.filePointer
            if (result != debugOutput.filePointer) {
                throwDebugMismatch()
            }
            return result
        }

        @Throws(IOException::class)
        override fun getChecksum(): Long {
            val result = output.checksum
            if (result != debugOutput.checksum) {
                throwDebugMismatch()
            }
            return result
        }

        @Throws(IOException::class)
        override fun writeByte(b: Byte) {
            output.writeByte(b)
            debugOutput.writeByte(b)
            filePointer
        }

        @Throws(IOException::class)
        override fun writeBytes(b: ByteArray, offset: Int, length: Int) {
            output.writeBytes(b, offset, length)
            debugOutput.writeBytes(b, offset, length)
            filePointer
        }
    }

    private inner class DebugIndexInput(private var name: String, context: IOContext, position: Long = 0L) :
            IndexInput("DebugIndexInput[$name]") {

        private var input = directory.openInput(name, context)
        private var debugInput = debugDirectory.openInput(name, context)

        init {
            input = directory.openInput(name, context)
            debugInput = debugDirectory.openInput(name, context)
            if (position > 0) {
                input.seek(position)
                debugInput.seek(position)
            }
        }

        @Throws(IOException::class)
        override fun close() {
            input.close()
            debugInput.close()
        }

        override fun getFilePointer(): Long {
            val result = input.filePointer
            if (result != debugInput.filePointer) {
                throwDebugMismatch()
            }
            return result
        }

        @Throws(IOException::class)
        override fun seek(pos: Long) {
            input.seek(pos)
            debugInput.seek(pos)
            if (input.filePointer != debugInput.filePointer) {
                throwDebugMismatch()
            }
        }

        override fun length(): Long {
            val result = input.length()
            if (result != debugInput.length()) {
                throwDebugMismatch()
            }
            return result
        }

        @Throws(IOException::class)
        override fun readByte(): Byte {
            val result = input.readByte()
            val debugByte = debugInput.readByte()
            if (result != debugByte) {
                throwDebugMismatch()
            }
            return result
        }

        @Throws(IOException::class)
        override fun readBytes(b: ByteArray, offset: Int, len: Int) {
            val before = filePointer
            input.readBytes(b, offset, len)
            val bytes = ByteArray(len)
            debugInput.readBytes(bytes, 0, len)
            val after = filePointer
            for (i in 0 until (after - before).toInt()) {
                if (bytes[i] != b[offset + i]) {
                    throwDebugMismatch()
                }
            }
        }

        override fun clone(): IndexInput {
            val result = super.clone() as DebugIndexInput
            result.name = name
            result.input = input.clone()
            result.debugInput = debugInput.clone()
            return result
        }

        @Throws(IOException::class)
        override fun slice(sliceDescription: String, offset: Long, length: Long): IndexInput {
            val result = super.clone() as DebugIndexInput
            result.name = name
            result.input = input.slice(sliceDescription, offset, length)
            result.debugInput = debugInput.slice(sliceDescription, offset, length)
            return result
        }
    }
}
