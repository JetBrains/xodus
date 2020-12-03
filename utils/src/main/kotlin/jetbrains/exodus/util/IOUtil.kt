/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.util

import java.io.*
import java.nio.file.Files
import kotlin.math.min

/**
 * Encapsulates utility methods for IO and compression routine.
 */
object IOUtil {

    const val READ_BUFFER_SIZE = 0x4000

    @JvmStatic
    val BUFFER_ALLOCATOR = ByteArraySpinAllocator(READ_BUFFER_SIZE)

    private const val BLOCK_SIZE = "exodus.io.blockSize"
    private val NO_FILES = arrayOf<File>()

    val blockSize: Long get() = java.lang.Long.getLong(BLOCK_SIZE, 0x1000)

    @JvmStatic
    fun isRemovableFile(file: File) =
            try {
                Files.getFileStore(file.toPath()).getAttribute("volume:isRemovable") == true
            } catch (_: Throwable) {
                false
            }

    @JvmStatic
    fun isCdromFile(file: File) =
            try {
                Files.getFileStore(file.toPath()).getAttribute("volume:isCdrom") == true
            } catch (_: Throwable) {
                false
            }

    @JvmStatic
    fun getAdjustedFileLength(file: File): Long =
            blockSize.let { blockSize ->
                (file.length() + blockSize - 1) / blockSize * blockSize
            }

    @JvmStatic
    fun getDirectorySize(dir: File, extension: String, recursive: Boolean): Long {
        var sum = 0L
        if (recursive) {
            for (childDir in listFiles(dir, FileFilter { obj -> obj.isDirectory })) {
                sum += getDirectorySize(childDir, extension,  /*always true*/recursive)
            }
        }
        for (file in listFiles(dir, FilenameFilter { _, name -> name.endsWith(extension) })) {
            sum += getAdjustedFileLength(file)
        }
        return sum
    }

    @JvmStatic
    fun copyStreams(source: InputStream,
                    target: OutputStream,
                    bufferAllocator: ByteArraySpinAllocator) {
        copyStreams(source, Long.MAX_VALUE, target, bufferAllocator)
    }

    @JvmStatic
    fun copyStreams(source: InputStream,
                    sourceLen: Long,
                    target: OutputStream,
                    bufferAllocator: ByteArraySpinAllocator) {
        val buffer = bufferAllocator.alloc()
        try {
            var totalRead: Long = 0
            while (totalRead < sourceLen) {
                val read = source.read(buffer)
                if (read < 0) break
                if (read > 0) {
                    target.write(buffer, 0, min(sourceLen - totalRead, read.toLong()).toInt())
                    totalRead += read.toLong()
                }
            }
        } finally {
            bufferAllocator.dispose(buffer)
        }
    }

    @JvmStatic
    fun deleteRecursively(dir: File) {
        for (file in listFiles(dir)) {
            if (file.isDirectory) {
                deleteRecursively(file)
            }
            deleteFile(file)
        }
    }

    @JvmStatic
    fun deleteFile(file: File) {
        if (!file.delete()) {
            file.deleteOnExit()
        }
    }

    @JvmStatic
    fun listFiles(directory: File): Array<File> = directory.listFiles() ?: NO_FILES

    @JvmStatic
    fun listFiles(directory: File, filter: FilenameFilter): Array<File> = directory.listFiles(filter) ?: NO_FILES

    @JvmStatic
    fun listFiles(directory: File, filter: FileFilter): Array<File> = directory.listFiles(filter) ?: NO_FILES

    @JvmOverloads
    @JvmStatic
    fun readFully(input: InputStream, bytes: ByteArray, len: Int = bytes.size): Int {
        var off = 0
        while (off < len) {
            val read = input.read(bytes, off, len - off)
            if (read < 0) {
                break
            }
            off += read
        }
        return off
    }
}
