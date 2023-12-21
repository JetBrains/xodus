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

class IvecsFileReader(
    filePath: Path,
    private val neighbourCountToRead: Int
): AutoCloseable {
    private val channel = FileChannel.open(filePath, StandardOpenOption.READ)

    private val neighbourCount: Int
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

fun readGroundTruth(filePath: Path, neighbourCountToRead: Int): Array<IntArray> = when (filePath.extension) {
    "ivecs" -> IvecsFileReader(filePath, neighbourCountToRead).use { reader ->
        reader.readAll()
    }
    else -> throw IllegalArgumentException("The file format ${filePath.fileName} is not supported")
}