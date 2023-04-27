/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.util

import jetbrains.exodus.ExodusException
import jetbrains.exodus.backup.BackupBean
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.Backupable
import jetbrains.exodus.backup.VirtualFileDescriptor
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.util.IOUtil.BUFFER_ALLOCATOR
import jetbrains.exodus.util.IOUtil.copyStreams
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.listFiles
import net.jpountz.lz4.LZ4Compressor
import net.jpountz.lz4.LZ4Factory
import net.jpountz.xxhash.XXHash32
import net.jpountz.xxhash.XXHashFactory
import org.apache.commons.compress.archivers.ArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarConstants
import org.apache.commons.compress.archivers.tar.TarUtils
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipEncoding
import org.apache.commons.compress.archivers.zip.ZipEncodingHelper
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.Deflater
import java.util.zip.GZIPOutputStream

object CompressBackupUtil {
    /**
     * LZ4 maximum data block size.
     */
    private const val LZ4_MAX_BLOCK_SIZE = 4 * 1024 * 1024

    /**
     * LZ4 magic number
     */
    private const val LZ4_MAGIC = 0x184D2204

    /**
     * LZ4 FLG byte:
     *
     *  1. version - `01`
     *  1. blocks are independent - `1`
     *  1. blocks checksum is provided - `1`
     *  1. content size is not provided - `0`
     *  1. content checksum is not provided - `0`
     *  1. reserved - `0`
     *  1. dictionary id is not set - @code 0}
     *
     */
    private const val LZ4_FLG: Byte = 112

    /**
     * LZ4 BD byte:
     *
     *  1. Reserved - `- 0`
     *  1. Block max size 4MB - `111`
     *  1. Reserved - `- 0000`
     *
     */
    private const val LZ4_BD_FLAG: Byte = 112
    private const val LZ4_END_MARK = 0
    private val logger = LoggerFactory.getLogger(CompressBackupUtil::class.java)

    /**
     * For specified [Backupable] `source`, creates backup file in the specified `backupRoot`
     * directory whose name is calculated using current timestamp and specified `backupNamePrefix` if it is not
     * `null`. Typically, `source` is an `Environment` or an `PersistentEntityStore`
     * instance. Set `zip = true` to create `.zip` backup file, otherwise `.tar.gz` file will be created.
     *
     *
     * `Environment` and `PersistentEntityStore` instances don't require any specific actions
     * (like, e.g., switching to read-only mode) to do backups and get consistent copies of data within backups files.
     * So backup can be performed on-the-fly not affecting database operations.
     *
     * @param source           an instance of [Backupable]
     * @param backupRoot       a directory which the backup file will be created in
     * @param backupNamePrefix prefix of the backup file name
     * @param zip              `true` to create `.zip` backup file, rather than `.tar.gz` one
     * @return backup file (either .zip or .tag.gz)
     * @throws Exception something went wrong
     */
    @Throws(Exception::class)
    @JvmStatic
    fun backup(
        source: Backupable, backupRoot: File,
        backupNamePrefix: String?, zip: Boolean
    ): File {
        if (!backupRoot.exists() && !backupRoot.mkdirs()) {
            throw IOException("Failed to create " + backupRoot.absolutePath)
        }
        val fileName: String = if (zip) {
            timeStampedZipFileName
        } else {
            timeStampedTarGzFileName
        }
        val backupFile = File(backupRoot, if (backupNamePrefix == null) fileName else backupNamePrefix + fileName)
        return backup(source, backupFile, zip)
    }

    /**
     * For specified [BackupBean], creates a backup file using [Backupable]s decorated by the bean
     * and the setting provided by the bean (backup path, prefix, zip or tar.gz).
     *
     *
     * Sets [System.currentTimeMillis] as backup start time, get it by
     * [BackupBean.getBackupStartTicks].
     *
     * @param backupBean bean holding one or several [Backupable]s and the settings
     * describing how to create backup file (backup path, prefix, zip or tar.gz)
     * @return backup file (either .zip or .tag.gz)
     * @throws Exception something went wrong
     * @see BackupBean
     *
     * @see BackupBean.getBackupPath
     * @see BackupBean.getBackupNamePrefix
     * @see BackupBean.getBackupToZip
     */
    @Throws(Exception::class)
    @JvmStatic
    fun backup(backupBean: BackupBean): File {
        backupBean.backupStartTicks = System.currentTimeMillis()
        return backup(
            backupBean,
            File(backupBean.backupPath), backupBean.backupNamePrefix, backupBean.backupToZip
        )
    }

    @Suppress("unused")
    @Throws(Exception::class)
    @JvmStatic
    fun parallelBackup(backupBean: BackupBean): File {
        backupBean.backupStartTicks = System.currentTimeMillis()
        return parallelBackup(
            backupBean,
            File(backupBean.backupPath), backupBean.backupNamePrefix
        )
    }

    /**
     * For specified [Backupable] `source` and `target` backup file, does backup.
     * Typically, `source` is an `Environment` or an `PersistentEntityStore`
     * instance. Set `zip = true` to create `.zip` backup file, otherwise `.tar.gz` file will be created.
     *
     *
     * `Environment` and `PersistentEntityStore` instances don't require any specific actions
     * (like, e.g., switching to read-only mode) to do backups and get consistent copies of data within backups files.
     * So backup can be performed on-the-fly not affecting database operations.
     *
     * @param source an instance of [Backupable]
     * @param target target backup file (either .zip or .tag.gz)
     * @param zip    `true` to create `.zip` backup file, rather than `.tar.gz` one
     * @return backup file the same as specified `target`
     * @throws Exception something went wrong
     */
    @Throws(Exception::class)
    @JvmStatic
    fun backup(
        source: Backupable,
        target: File, zip: Boolean
    ): File {
        if (target.exists()) {
            throw IOException("Backup file already exists:" + target.absolutePath)
        }
        val strategy = source.backupStrategy
        strategy.beforeBackup()
        try {
            BufferedOutputStream(Files.newOutputStream(target.toPath())).use { output ->
                val archive: ArchiveOutputStream = if (zip) {
                    val zipArchive = ZipArchiveOutputStream(output)
                    zipArchive.setLevel(Deflater.BEST_COMPRESSION)
                    zipArchive
                } else {
                    TarArchiveOutputStream(GZIPOutputStream(output))
                }
                archive.use { aos ->
                    for (fd in strategy.contents) {
                        if (strategy.isInterrupted) {
                            break
                        }
                        if (fd.hasContent()) {
                            val fileSize = fd.fileSize.coerceAtMost(strategy.acceptFile(fd))
                            if (fileSize > 0L) {
                                archiveFile(aos, fd, fileSize)
                            }
                        }
                    }
                }
                if (strategy.isInterrupted) {
                    logger.info("Backup interrupted, deleting \"" + target.name + "\"...")
                    deleteFile(target)
                } else {
                    RandomAccessFile(target, "rw").use { file -> file.fd.sync() }
                    logger.info("Backup file \"" + target.name + "\" created.")
                }
            }
        } catch (t: Throwable) {
            strategy.onError(t)
            throw ExodusException.toExodusException(t, "Backup failed")
        } finally {
            strategy.afterBackup()
        }
        return target
    }

    @Throws(Exception::class)
    @JvmStatic
    fun parallelBackup(
        source: Backupable, backupRoot: File,
        backupNamePrefix: String?
    ): File {
        if (!backupRoot.exists() && !backupRoot.mkdirs()) {
            throw IOException("Failed to create " + backupRoot.absolutePath)
        }
        val fileName = timeStampedTarLz4FileName
        val backupFile = File(backupRoot, if (backupNamePrefix == null) fileName else backupNamePrefix + fileName)
        return parallelBackup(source, backupFile)
    }

    @Throws(Exception::class)
    @JvmStatic
    fun parallelBackup(
        source: Backupable,
        target: File
    ): File {
        if (target.exists()) {
            throw IOException("Backup file already exists:" + target.absolutePath)
        }
        val strategy = source.backupStrategy
        try {
            strategy.beforeBackup()
            val hashFactory = XXHashFactory.fastestInstance()
            val hash32 = hashFactory.hash32()
            val compressedFilePosition = AtomicLong()
            FileChannel.open(
                target.toPath(), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
            ).use { backupChannel ->
                writeLZ4EmptyFrame(backupChannel, hash32)
                writeLZ4FrameHeader(
                    backupChannel,
                    hash32,
                    compressedFilePosition
                )
            }
            val zeroCompression = strategy.isEncrypted
            if (zeroCompression && logger.isInfoEnabled) {
                logger.info("Backup content is encrypted and will not be compressed.")
            }
            val processors = Runtime.getRuntime().availableProcessors()
            val threadLimit = 2.coerceAtLeast(processors / 4)
            val freeMemory = Runtime.getRuntime().freeMemory()
            val memoryLimit = (freeMemory / 6).coerceAtLeast(64L * 1024 * 1024)
            val compressorsLimit = threadLimit.toLong().coerceAtMost(memoryLimit / LZ4_MAX_BLOCK_SIZE).toInt()
            if (logger.isInfoEnabled) {
                logger.info(
                    String.format(
                        "%,d Mb of free heap memory was detected into the system, %,d Mb is allowed" +
                                " to be used for backup.", freeMemory / (1024 * 1024), memoryLimit / (1024 * 1024)
                    )
                )
                logger.info(
                    String.format(
                        "%d processors were detected. %d is allowed to be used for backup.",
                        processors, threadLimit
                    )
                )
                logger.info(String.format("Amount of threads used for backup is set to %d.", compressorsLimit))
            }
            val streamMachinery = Executors.newFixedThreadPool(compressorsLimit) { r: Runnable? ->
                val thread = Thread(r)
                thread.isDaemon = true
                thread.name = "Parallel compressed backup thread"
                thread
            }
            val generatorStopped = AtomicBoolean()
            val descriptors = ConcurrentLinkedQueue<Pair<VirtualFileDescriptor, Long>>()
            val queueCapacity: Int = Int.MAX_VALUE.toLong().coerceAtMost(
                1024L.coerceAtLeast(freeMemory / 100 / 512)).toInt()
            val queueSemaphore = Semaphore(queueCapacity)
            if (logger.isInfoEnabled) {
                logger.info(String.format("Capacity of the backup queue is set to %d files", queueCapacity))
            }
            val threads = ArrayList<Future<Void?>>()
            val zipEncoding = ZipEncodingHelper.getZipEncoding(null)
            val factory = LZ4Factory.safeInstance()
            val compressor = factory.fastCompressor()
            val maxCompressedBlockSize = 2 * Integer.BYTES + compressor.maxCompressedLength(LZ4_MAX_BLOCK_SIZE)
            for (i in 0 until compressorsLimit) {
                threads.add(streamMachinery.submit<Void?> {
                    FileChannel.open(target.toPath(), StandardOpenOption.WRITE).use { backupChannel ->
                        val buffer =
                            ByteBuffer.allocate(LZ4_MAX_BLOCK_SIZE)
                        val compressionBufferSize = 2 * maxCompressedBlockSize
                        val compressedBuffer =
                            ByteBuffer.allocateDirect(compressionBufferSize).order(ByteOrder.LITTLE_ENDIAN)
                        while (true) {
                            val genStopped = generatorStopped.get()
                            val pair =
                                descriptors.poll()
                            if (pair != null) {
                                queueSemaphore.release(1)
                                val fd = pair.first
                                val fileSize = pair.second
                                if (fd.hasContent()) {
                                    fd.inputStream.use { fileStream ->
                                        var bytesWritten: Long = 0
                                        var fileIndex = 0
                                        while (bytesWritten < fileSize) {
                                            val chunkSize = (buffer.remaining() -
                                                    TarConstants.DEFAULT_RCDSIZE).toLong()
                                                .coerceAtMost(fileSize - bytesWritten).toInt()
                                            if (chunkSize > 0) {
                                                val fullPath: String
                                                val fdName = fd.name
                                                val extensionIndex = fdName.lastIndexOf('.')
                                                fullPath =
                                                    if (extensionIndex >= 0 && extensionIndex < fdName.length - 1) {
                                                        String.format(
                                                            "%s%s-%08X%s",
                                                            fd.path,
                                                            fdName.substring(0, extensionIndex),
                                                            fileIndex,
                                                            fdName.substring(extensionIndex)
                                                        )
                                                    } else {
                                                        String.format(
                                                            "%s%s-%08X",
                                                            fd.path,
                                                            fdName,
                                                            fileIndex
                                                        )
                                                    }
                                                writeTarFileHeader(
                                                    buffer,
                                                    fullPath,
                                                    chunkSize.toLong(),
                                                    fd.timeStamp,
                                                    zipEncoding
                                                )
                                                var bytesRead = 0
                                                val bufferArray = buffer.array()
                                                val bufferOffset = buffer.arrayOffset()
                                                var bufferPosition = buffer.position()
                                                while (bytesRead < chunkSize) {
                                                    val r = fileStream.read(
                                                        bufferArray, bufferOffset + bufferPosition,
                                                        chunkSize - bytesRead
                                                    )
                                                    if (r == -1) {
                                                        break
                                                    }
                                                    bufferPosition += r
                                                    bytesRead += r
                                                }
                                                if (bytesRead > 0) {
                                                    bytesWritten += bytesRead.toLong()
                                                    buffer.position(bufferPosition)
                                                } else {
                                                    throw ExodusException("Invalid file size")
                                                }
                                                tarPadding(buffer)
                                                fileIndex++
                                            } else {
                                                writeLZ4Block(
                                                    buffer, compressedBuffer, compressor, hash32,
                                                    zeroCompression
                                                )
                                                if (compressedBuffer.remaining() < maxCompressedBlockSize) {
                                                    writeChunk(
                                                        compressedBuffer,
                                                        compressedFilePosition,
                                                        backupChannel
                                                    )
                                                }
                                            }
                                        }
                                    }
                                }
                            } else if (genStopped) {
                                break
                            }
                        }
                        tarPadding(buffer)
                        writeLZ4Block(
                            buffer,
                            compressedBuffer,
                            compressor,
                            hash32,
                            zeroCompression
                        )
                        writeChunk(
                            compressedBuffer,
                            compressedFilePosition,
                            backupChannel
                        )
                    }
                    null
                })
            }
            for (fd in strategy.contents) {
                if (strategy.isInterrupted) {
                    break
                }
                if (fd.hasContent()) {
                    val fileSize = fd.fileSize.coerceAtMost(strategy.acceptFile(fd))
                    if (fileSize > 0) {
                        queueSemaphore.acquire()
                        descriptors.offer(Pair(fd, fileSize))
                    }
                }
            }
            generatorStopped.set(true)
            try {
                for (thread in threads) {
                    thread.get()
                }
            } finally {
                streamMachinery.shutdown()
            }
            FileChannel.open(target.toPath(), StandardOpenOption.WRITE).use { backupChannel ->
                val lastBlock = ByteBuffer.allocate(2 * TarConstants.DEFAULT_RCDSIZE)
                val compressedBuffer = ByteBuffer.allocateDirect(
                    2 * Integer.BYTES +
                            compressor.maxCompressedLength(lastBlock.remaining())
                ).order(ByteOrder.LITTLE_ENDIAN)
                lastBlock.position(lastBlock.remaining())
                writeLZ4Block(
                    lastBlock,
                    compressedBuffer,
                    compressor,
                    hash32,
                    false
                )
                writeChunk(
                    compressedBuffer,
                    compressedFilePosition,
                    backupChannel
                )
                writeLZ4FrameEndMark(backupChannel)
            }
            if (strategy.isInterrupted) {
                logger.info("Backup interrupted, deleting \"" + target.name + "\"...")
                deleteFile(target)
            } else {
                logger.info("Backup file \"" + target.name + "\" created.")
            }
        } catch (t: Throwable) {
            strategy.onError(t)
            throw ExodusException.toExodusException(t, "Backup failed")
        } finally {
            strategy.afterBackup()
        }
        return target
    }

    private fun tarPadding(buffer: ByteBuffer) {
        val reminder = TarConstants.DEFAULT_RCDSIZE -
                buffer.position() and TarConstants.DEFAULT_RCDSIZE - 1
        buffer.position(buffer.position() + reminder)
    }

    @Throws(IOException::class)
    private fun writeTarFileHeader(
        buffer: ByteBuffer, path: String,
        size: Long,
        ts: Long,
        zipEncoding: ZipEncoding
    ) {
        val arrayOffset = buffer.arrayOffset()
        val position = buffer.position()
        val array = buffer.array()
        assert(position % TarConstants.DEFAULT_RCDSIZE == 0)
        var offset = position + arrayOffset

        //file path
        offset = TarUtils.formatNameBytes(
            normalizeTarFileName(path), array, offset, TarConstants.NAMELEN,
            zipEncoding
        )
        offset = TarUtils.formatLongOctalOrBinaryBytes(
            TarArchiveEntry.DEFAULT_FILE_MODE.toLong(), array, offset,
            TarConstants.MODELEN
        )
        Arrays.fill(array, offset, offset + TarConstants.UIDLEN + TarConstants.GIDLEN, 0.toByte())
        //user id
        offset += TarConstants.UIDLEN
        //group id
        offset += TarConstants.GIDLEN
        offset = TarUtils.formatLongOctalOrBinaryBytes(size, array, offset, TarConstants.SIZELEN)
        //convert from java time to the tar time.
        offset = TarUtils.formatLongOctalOrBinaryBytes(ts / 1000, array, offset, TarConstants.MODTIMELEN)
        val csOffset = offset
        Arrays.fill(array, offset, offset + TarConstants.CHKSUMLEN, ' '.code.toByte())
        offset += TarConstants.CHKSUMLEN
        array[offset++] = TarConstants.LF_NORMAL
        val chk = computeTarCheckSum(array, arrayOffset + position, offset - (position + arrayOffset))
        TarUtils.formatCheckSumOctalBytes(chk, array, csOffset, TarConstants.CHKSUMLEN)
        Arrays.fill(array, offset, arrayOffset + position + TarConstants.DEFAULT_RCDSIZE, 0.toByte())
        buffer.position(position + TarConstants.DEFAULT_RCDSIZE)
    }

    private fun computeTarCheckSum(buf: ByteArray, off: Int, len: Int): Long {
        var sum: Long = 0
        val end = off + len
        for (i in off until end) {
            sum += (0xFF and buf[i].toInt()).toLong()
        }
        return sum
    }

    private fun writeLZ4Block(
        buffer: ByteBuffer, compressionBuffer: ByteBuffer,
        compressor: LZ4Compressor, hash32: XXHash32,
        zeroCompression: Boolean
    ) {
        if (buffer.position() == 0) {
            return
        }
        buffer.flip()
        val startPosition = compressionBuffer.position()
        compressionBuffer.position(startPosition + Integer.BYTES)
        var len: Int
        var uncompressed: Boolean
        if (!zeroCompression) {
            compressor.compress(buffer, compressionBuffer)
            val endPosition = compressionBuffer.position()
            len = endPosition - (startPosition + Integer.BYTES)
            uncompressed = false
            if (len > LZ4_MAX_BLOCK_SIZE) {
                compressionBuffer.position(startPosition + Integer.BYTES)
                buffer.flip()
                compressionBuffer.put(buffer)
                uncompressed = true
                len = buffer.limit()
            }
        } else {
            uncompressed = true
            compressionBuffer.put(buffer)
            len = buffer.limit()
        }
        val hash = hash32.hash(compressionBuffer, startPosition + Integer.BYTES, len, 0)
        compressionBuffer.putInt(hash)
        if (uncompressed) {
            compressionBuffer.putInt(startPosition, 1 shl Integer.SIZE - 1 or len)
        } else {
            compressionBuffer.putInt(startPosition, len)
        }
        buffer.clear()
    }

    @Throws(IOException::class)
    private fun writeLZ4FrameHeader(channel: FileChannel, hash32: XXHash32, compressedPosition: AtomicLong) {
        val buffer = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN)
        writeLZ4FrameHeader(hash32, buffer)
        buffer.flip()
        compressedPosition.set(buffer.remaining().toLong())
        while (buffer.remaining() > 0) {
            channel.write(buffer)
        }
    }

    private fun writeLZ4FrameHeader(hash32: XXHash32, buffer: ByteBuffer) {
        //magic number
        buffer.putInt(LZ4_MAGIC)

        //frame descriptor
        val position = buffer.position()
        buffer.put(LZ4_FLG)
        buffer.put(LZ4_BD_FLAG)
        val hash = hash32.hash(buffer, position, 2, 0)
        buffer.put((hash shr 8 and 0xFF).toByte())
    }

    @Throws(IOException::class)
    private fun writeLZ4FrameEndMark(channel: FileChannel) {
        val buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
        writeLZ4FrameEndMark(buffer)
        buffer.flip()
        while (buffer.remaining() > 0) {
            channel.write(buffer)
        }
    }

    private fun writeLZ4FrameEndMark(buffer: ByteBuffer) {
        buffer.putInt(LZ4_END_MARK)
    }

    @Throws(IOException::class)
    private fun writeLZ4EmptyFrame(channel: FileChannel, hash32: XXHash32) {
        val buffer = ByteBuffer.allocate(
            2 * Integer.BYTES +
                    3 * java.lang.Byte.BYTES
        ).order(ByteOrder.LITTLE_ENDIAN)
        writeLZ4FrameHeader(hash32, buffer)
        writeLZ4FrameEndMark(buffer)
        while (buffer.remaining() > 0) {
            channel.write(buffer)
        }
    }

    @Throws(IOException::class)
    private fun writeChunk(
        compressedBuffer: ByteBuffer,
        compressedFilePosition: AtomicLong,
        fileChannel: FileChannel
    ) {
        if (compressedBuffer.position() == 0) {
            return
        }
        compressedBuffer.flip()
        var position: Long
        do {
            position = compressedFilePosition.get()
        } while (!compressedFilePosition.compareAndSet(position, position + compressedBuffer.limit()))
        fileChannel.position(position)
        while (compressedBuffer.remaining() > 0) {
            fileChannel.write(compressedBuffer)
        }
        compressedBuffer.clear()
    }

    private fun normalizeTarFileName(fileName: String): String {
        var resultFileName = fileName
        val property = System.getProperty("os.name")
        if (property != null) {
            val osName = property.lowercase()
            if (osName.startsWith("windows")) {
                if (resultFileName.length > 2) {
                    val ch1 = resultFileName[0]
                    val ch2 = resultFileName[1]
                    if (ch2 == ':' && (ch1 in 'a'..'z' || ch1 in 'A'..'Z')) {
                        resultFileName = resultFileName.substring(2)
                    }
                }
            } else if (osName.contains("netware")) {
                val colon = resultFileName.indexOf(':')
                if (colon != -1) {
                    resultFileName = resultFileName.substring(colon + 1)
                }
            }
        }
        resultFileName = resultFileName.replace(File.separatorChar, '/')
        while (resultFileName.startsWith("/")) {
            resultFileName = resultFileName.substring(1)
        }
        return resultFileName
    }

    private val timeStampedTarGzFileName: String
        get() {
            val builder = StringBuilder(30)
            appendTimeStamp(builder)
            builder.append(".tar.gz")
            return builder.toString()
        }
    private val timeStampedTarLz4FileName: String
        get() {
            val builder = StringBuilder(30)
            appendTimeStamp(builder)
            builder.append(".tar.lz4")
            return builder.toString()
        }
    private val timeStampedZipFileName: String
        get() {
            val builder = StringBuilder(30)
            appendTimeStamp(builder)
            builder.append(".zip")
            return builder.toString()
        }

    @Throws(IOException::class)
    @JvmStatic
    fun postProcessBackup(restoreDir: Path) {
        Files.walkFileTree(restoreDir, object : FileVisitor<Path> {
            private val filePeaces = HashMap<Path, TreeMap<Path, ArrayList<Path?>>>()
            override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                val name = file.fileName.toString()
                val fileExtIndex = name.lastIndexOf(".")
                val fileIndex: Int
                val indexStr: String
                val fileName: String
                if (fileExtIndex >= 0 && fileExtIndex < name.length - 1) {
                    indexStr = name.substring(fileExtIndex - 8, fileExtIndex)
                    fileName = name.substring(0, fileExtIndex - 9) + name.substring(fileExtIndex)
                } else {
                    indexStr = name.substring(name.length - 8)
                    fileName = name.substring(0, name.length - 9)
                }
                fileIndex = indexStr.toInt(16)
                val absolutePath = file.toRealPath()
                val currentDirectory = absolutePath.parent
                val realFileName = currentDirectory.resolve(fileName)
                val directoryFiles = filePeaces.computeIfAbsent(
                    currentDirectory
                ) { _: Path? -> TreeMap() }
                val pieces = directoryFiles.computeIfAbsent(
                    realFileName
                ) { _: Path? -> ArrayList() }
                while (pieces.size <= fileIndex) {
                    pieces.add(null)
                }
                pieces[fileIndex] = absolutePath
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exc: IOException): FileVisitResult {
                throw IOException(exc)
            }

            @Throws(IOException::class)
            override fun postVisitDirectory(dir: Path, exc: IOException?): FileVisitResult {
                if (exc != null) {
                    throw IOException(exc)
                }
                val directoryFiles = filePeaces.remove(dir)
                directoryFiles?.forEach { (fullFile: Path?, filePieces: ArrayList<Path?>) ->
                    try {
                        try {
                            Files.move(filePieces[0]!!, fullFile, StandardCopyOption.ATOMIC_MOVE)
                        } catch (e: AtomicMoveNotSupportedException) {
                            Files.move(filePieces[0]!!, fullFile)
                        }
                        if (filePieces.size == 1) {
                            return@forEach
                        }
                        FileChannel.open(fullFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
                            .use { channel ->
                                for (piece in filePieces.subList(1, filePieces.size)) {
                                    var bytesWritten: Long = 0
                                    val bytesToWrite = Files.size(piece!!)
                                    FileChannel.open(piece, StandardOpenOption.READ).use { pieceChannel ->
                                        while (bytesWritten < bytesToWrite) {
                                            val w = pieceChannel.transferTo(
                                                bytesWritten,
                                                bytesToWrite - bytesWritten, channel
                                            )
                                            bytesWritten += w
                                        }
                                    }
                                    Files.delete(piece)
                                }
                            }
                    } catch (e: IOException) {
                        throw ExodusException("Error during postprocessing of backup files.", e)
                    }
                }
                return FileVisitResult.CONTINUE
            }
        })
    }

    /**
     * Compresses the content of source and stores newly created archive in dest.
     * In case source is a directory, it will be compressed recursively.
     *
     * @param source file or folder to be archived. Should exist on method call.
     * @param dest   path to the archive to be created. Should not exist on method call.
     * @throws IOException           in case of any issues with underlying store.
     * @throws FileNotFoundException in case source does not exist.
     */
    @Throws(IOException::class)
    @JvmStatic
    fun tar(source: File, dest: File) {
        require(source.exists()) { "No source file or folder exists: " + source.absolutePath }
        require(!dest.exists()) { "Destination refers to existing file or folder: " + dest.absolutePath }
        try {
            TarArchiveOutputStream(
                GZIPOutputStream(
                    BufferedOutputStream(Files.newOutputStream(dest.toPath())), 0x1000
                )
            ).use { tarOut ->
                doTar(
                    "",
                    source,
                    tarOut
                )
            }
        } catch (e: IOException) {
            deleteFile(dest) // operation filed, let's remove the destination archive
            throw e
        }
    }

    @Throws(IOException::class)
    private fun doTar(
        pathInArchive: String,
        source: File,
        tarOut: TarArchiveOutputStream
    ) {
        if (source.isDirectory) {
            for (file in listFiles(source)) {
                doTar(pathInArchive + source.name + File.separator, file, tarOut)
            }
        } else {
            archiveFile(tarOut, BackupStrategy.FileDescriptor(source, pathInArchive), source.length())
        }
    }

    /**
     * Adds the file to the tar archive represented by output stream. It's caller's responsibility to close output stream
     * properly.
     *
     * @param out      target archive.
     * @param source   file to be added.
     * @param fileSize size of the file (which is known in most cases).
     * @throws IOException in case of any issues with underlying store.
     */
    @Throws(IOException::class)
    @JvmStatic
    fun archiveFile(
        out: ArchiveOutputStream,
        source: VirtualFileDescriptor,
        fileSize: Long
    ) {
        require(source.hasContent()) { "Provided source is not a file: " + source.path }
        when (out) {
            is TarArchiveOutputStream -> {
                val entry = TarArchiveEntry(source.path + source.name)
                entry.size = fileSize
                entry.setModTime(source.timeStamp)
                out.putArchiveEntry(entry)
            }

            is ZipArchiveOutputStream -> {
                val entry = ZipArchiveEntry(source.path + source.name)
                entry.size = fileSize
                entry.time = source.timeStamp
                out.putArchiveEntry(entry)
            }

            else -> {
                throw IOException("Unknown archive output stream")
            }
        }
        val input = source.inputStream
        try {
            copyStreams(input, fileSize, out, BUFFER_ALLOCATOR)
        } finally {
            if (source.shouldCloseStream()) {
                input.close()
            }
        }
        out.closeArchiveEntry()
    }

    @Suppress("unused")
    @Throws(IOException::class)
    @JvmStatic
    fun archiveFile(
        out: ArchiveOutputStream,
        compressionEntry: CompressionEntry
    ) {
        when (out) {
            is TarArchiveOutputStream -> {
                val entry = TarArchiveEntry(compressionEntry.path + compressionEntry.name)
                entry.size = compressionEntry.fileSize
                entry.setModTime(compressionEntry.timeStamp)
                out.putArchiveEntry(entry)
            }

            is ZipArchiveOutputStream -> {
                val entry = ZipArchiveEntry(compressionEntry.path + compressionEntry.name)
                entry.size = compressionEntry.fileSize
                entry.time = compressionEntry.timeStamp
                out.putArchiveEntry(entry)
            }

            else -> {
                throw IOException("Unknown archive output stream")
            }
        }
        try {
            copyStreams(compressionEntry.input, compressionEntry.fileSize, out, BUFFER_ALLOCATOR)
        } finally {
            if (compressionEntry.shouldCloseStream) {
                compressionEntry.input.close()
            }
        }
        out.closeArchiveEntry()
    }

    private fun appendTimeStamp(builder: StringBuilder) {
        val now = Calendar.getInstance()
        builder.append(now[Calendar.YEAR])
        builder.append('-')
        appendZeroPadded(builder, now[Calendar.MONTH] + 1)
        builder.append('-')
        appendZeroPadded(builder, now[Calendar.DAY_OF_MONTH])
        builder.append('-')
        appendZeroPadded(builder, now[Calendar.HOUR_OF_DAY])
        builder.append('-')
        appendZeroPadded(builder, now[Calendar.MINUTE])
        builder.append('-')
        appendZeroPadded(builder, now[Calendar.SECOND])
    }

    private fun appendZeroPadded(builder: StringBuilder, value: Int) {
        if (value < 10) {
            builder.append('0')
        }
        builder.append(value)
    }

    class CompressionEntry private constructor(
        val input: InputStream,
        val path: String,
        val name: String,
        val timeStamp: Long,
        val fileSize: Long,
        val shouldCloseStream: Boolean
    )
}
