/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.vectoriadb.index.bench

import java.io.BufferedOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.extension
import kotlin.math.min

class IvecsFileReader(
    filePath: Path,
    maxNeighbourCount: Int
): AutoCloseable {
    private val channel = FileChannel.open(filePath, StandardOpenOption.READ)

    private val neighbourCount: Int
    private val neighbourCountToRead: Int
    private val recordSize: Int
    private val recordCount: Int

    private val recordSizeToRead: Int
    private val buffer: ByteBuffer

    init {
        val neighbourCountBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
        neighbourCountBuffer.order(ByteOrder.LITTLE_ENDIAN)
        while (neighbourCountBuffer.hasRemaining()) {
            channel.read(neighbourCountBuffer, 0L + neighbourCountBuffer.position())
        }
        neighbourCountBuffer.rewind()
        neighbourCount = neighbourCountBuffer.getInt()
        neighbourCountToRead = min(neighbourCount, maxNeighbourCount)
        check(neighbourCount >= neighbourCountToRead) { "The file contains fewer neighbours $neighbourCount than required $neighbourCountToRead" }

        recordSize = Int.SIZE_BYTES * neighbourCount + Int.SIZE_BYTES
        recordCount = (channel.size() / recordSize).toInt()

        recordSizeToRead = Int.SIZE_BYTES * neighbourCountToRead + Int.SIZE_BYTES
        buffer = ByteBuffer.allocate(recordSizeToRead)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
    }

    fun read(idx: Int): IntArray {
        val vector = IntArray(neighbourCountToRead)
        while (buffer.hasRemaining()) {
            channel.read(buffer, recordSize.toLong() * idx + buffer.position())
        }
        buffer.rewind()

        check(buffer.getInt() == neighbourCount) { "Neighbour count mismatch" }

        for (i in vector.indices) {
            vector[i] = buffer.getInt()
        }
        buffer.clear()
        return vector
    }

    fun readAll(): Array<IntArray> {
        return Array(recordCount) { i -> read(i) }
    }

    override fun close() {
        channel.close()
    }
}

class IvecsFileWriter(
    filePath: Path,
    private val neighbourCount: Int
): AutoCloseable {

    private val outputStream: DataOutputStream = DataOutputStream(BufferedOutputStream(Files.newOutputStream(filePath), 64 * 1024 * 1024))

    fun write(neighbours: IntArray) {
        check(neighbours.size == neighbourCount) { "neighbour count missmatch" }
        // .ivecs files are in Little-Endian format
        // There is no standard way to make DataOutputStream Little-Endian.
        // So, we use Integer.reverseBytes(...) to hack it
        outputStream.writeInt(Integer.reverseBytes(neighbourCount))
        for (neighbour in neighbours) {
            outputStream.writeInt(Integer.reverseBytes(neighbour))
        }
    }

    override fun close() {
        outputStream.close()
    }
}

fun readGroundTruth(filePath: Path, maxNeighbourCount: Int = Int.MAX_VALUE): Array<IntArray> = when (filePath.extension) {
    "ivecs" -> IvecsFileReader(filePath, maxNeighbourCount).use { reader ->
        reader.readAll()
    }
    else -> throw IllegalArgumentException("The file format ${filePath.fileName} is not supported")
}