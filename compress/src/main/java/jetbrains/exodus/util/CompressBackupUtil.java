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
package jetbrains.exodus.util;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.backup.BackupBean;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.Backupable;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.env.Environment;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.archivers.tar.TarUtils;
import org.apache.commons.compress.archivers.zip.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class CompressBackupUtil {
    /**
     * LZ4 maximum data block size.
     */
    private static final int LZ4_MAX_BLOCK_SIZE = 4 * 1024 * 1024;

    /**
     * LZ4 magic number
     */
    private static final int LZ4_MAGIC = 0x184D2204;
    /**
     * LZ4 FLG byte:
     * <ol>
     * <li>version - {@code 01}</li>
     * <li>blocks are independent - {@code 1} </li>
     * <li>blocks checksum is provided - {@code 1}</li>
     * <li>content size is not provided - {@code 0}</li>
     * <li>content checksum is not provided - {@code 0}</li>
     * <li>reserved - {@code 0}</li>
     * <li>dictionary id is not set - @code 0}</li>
     * </ol>
     */
    private static final byte LZ4_FLG = 0b01_1_1_0_0_0_0;

    /**
     * LZ4 BD byte:
     * <ol>
     *     <li>Reserved - {@code - 0}</li>
     *     <li>Block max size 4MB - {@code 111}</li>
     *     <li>Reserved - {@code - 0000}</li>
     * </ol>
     */
    private static final byte LZ4_BD_FLAG = 0b0_111_0000;

    private static final int LZ4_END_MARK = 0;

    private static final Logger logger = LoggerFactory.getLogger(CompressBackupUtil.class);

    private CompressBackupUtil() {
    }

    /**
     * For specified {@linkplain Backupable} {@code source}, creates backup file in the specified {@code backupRoot}
     * directory whose name is calculated using current timestamp and specified {@code backupNamePrefix} if it is not
     * {@code null}. Typically, {@code source} is an {@linkplain Environment} or an {@linkplain PersistentEntityStore}
     * instance. Set {@code zip = true} to create {@code .zip} backup file, otherwise {@code .tar.gz} file will be created.
     *
     * <p>{@linkplain Environment} and {@linkplain PersistentEntityStore} instances don't require any specific actions
     * (like, e.g., switching to read-only mode) to do backups and get consistent copies of data within backups files.
     * So backup can be performed on-the-fly not affecting database operations.
     *
     * @param source           an instance of {@linkplain Backupable}
     * @param backupRoot       a directory which the backup file will be created in
     * @param backupNamePrefix prefix of the backup file name
     * @param zip              {@code true} to create {@code .zip} backup file, rather than {@code .tar.gz} one
     * @return backup file (either .zip or .tag.gz)
     * @throws Exception something went wrong
     */
    @NotNull
    public static File backup(@NotNull final Backupable source, @NotNull final File backupRoot,
                              @Nullable final String backupNamePrefix, final boolean zip) throws Exception {
        if (!backupRoot.exists() && !backupRoot.mkdirs()) {
            throw new IOException("Failed to create " + backupRoot.getAbsolutePath());
        }
        final String fileName;
        if (zip) {
            fileName = getTimeStampedZipFileName();
        } else {
            fileName = getTimeStampedTarGzFileName();
        }
        final File backupFile = new File(backupRoot, backupNamePrefix == null ? fileName : backupNamePrefix + fileName);
        return backup(source, backupFile, zip);
    }

    /**
     * For specified {@linkplain BackupBean}, creates a backup file using {@linkplain Backupable}s decorated by the bean
     * and the setting provided by the bean (backup path, prefix, zip or tar.gz).
     * <p>
     * Sets {@linkplain System#currentTimeMillis()} as backup start time, get it by
     * {@linkplain BackupBean#getBackupStartTicks()}.
     *
     * @param backupBean bean holding one or several {@linkplain Backupable}s and the settings
     *                   describing how to create backup file (backup path, prefix, zip or tar.gz)
     * @return backup file (either .zip or .tag.gz)
     * @throws Exception something went wrong
     * @see BackupBean
     * @see BackupBean#getBackupPath()
     * @see BackupBean#getBackupNamePrefix()
     * @see BackupBean#getBackupToZip()
     */
    @NotNull
    public static File backup(@NotNull final BackupBean backupBean) throws Exception {
        backupBean.setBackupStartTicks(System.currentTimeMillis());
        return backup(backupBean,
                new File(backupBean.getBackupPath()), backupBean.getBackupNamePrefix(), backupBean.getBackupToZip());
    }

    /**
     * For specified {@linkplain Backupable} {@code source} and {@code target} backup file, does backup.
     * Typically, {@code source} is an {@linkplain Environment} or an {@linkplain PersistentEntityStore}
     * instance. Set {@code zip = true} to create {@code .zip} backup file, otherwise {@code .tar.gz} file will be created.
     *
     * <p>{@linkplain Environment} and {@linkplain PersistentEntityStore} instances don't require any specific actions
     * (like, e.g., switching to read-only mode) to do backups and get consistent copies of data within backups files.
     * So backup can be performed on-the-fly not affecting database operations.
     *
     * @param source an instance of {@linkplain Backupable}
     * @param target target backup file (either .zip or .tag.gz)
     * @param zip    {@code true} to create {@code .zip} backup file, rather than {@code .tar.gz} one
     * @return backup file the same as specified {@code target}
     * @throws Exception something went wrong
     */
    @NotNull
    public static File backup(@NotNull final Backupable source,
                              @NotNull final File target, final boolean zip) throws Exception {
        if (target.exists()) {
            throw new IOException("Backup file already exists:" + target.getAbsolutePath());
        }
        final BackupStrategy strategy = source.getBackupStrategy();
        strategy.beforeBackup();
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(target.toPath()))) {
            final ArchiveOutputStream archive;
            if (zip) {
                final ZipArchiveOutputStream zipArchive = new ZipArchiveOutputStream(output);
                zipArchive.setLevel(Deflater.BEST_COMPRESSION);
                archive = zipArchive;
            } else {
                archive = new TarArchiveOutputStream(new GZIPOutputStream(output));
            }
            try (ArchiveOutputStream aos = archive) {
                for (final VirtualFileDescriptor fd : strategy.getContents()) {
                    if (strategy.isInterrupted()) {
                        break;
                    }
                    if (fd.hasContent()) {
                        final long fileSize = Math.min(fd.getFileSize(), strategy.acceptFile(fd));
                        if (fileSize > 0L) {
                            archiveFile(aos, fd, fileSize);
                        }
                    }
                }
            }
            if (strategy.isInterrupted()) {
                logger.info("Backup interrupted, deleting \"" + target.getName() + "\"...");
                IOUtil.deleteFile(target);
            } else {
                try (var file = new RandomAccessFile(target, "rw")) {
                    file.getFD().sync();
                }
                logger.info("Backup file \"" + target.getName() + "\" created.");
            }
        } catch (Throwable t) {
            strategy.onError(t);
            throw ExodusException.toExodusException(t, "Backup failed");
        } finally {
            strategy.afterBackup();
        }
        return target;
    }

    @NotNull
    public static File parallelBackup(@NotNull final Backupable source, @NotNull final File backupRoot,
                                      @Nullable final String backupNamePrefix) throws Exception {
        if (!backupRoot.exists() && !backupRoot.mkdirs()) {
            throw new IOException("Failed to create " + backupRoot.getAbsolutePath());
        }

        final String fileName = getTimeStampedTarLz4FileName();
        final File backupFile = new File(backupRoot, backupNamePrefix == null ? fileName : backupNamePrefix + fileName);
        return parallelBackup(source, backupFile);
    }


    @NotNull
    public static File parallelBackup(@NotNull final Backupable source,
                                      @NotNull final File target) throws Exception {
        if (target.exists()) {
            throw new IOException("Backup file already exists:" + target.getAbsolutePath());
        }

        final BackupStrategy strategy = source.getBackupStrategy();
        try {
            strategy.beforeBackup();

            final XXHashFactory hashFactory = XXHashFactory.fastestInstance();
            final XXHash32 hash32 = hashFactory.hash32();
            final AtomicLong compressedFilePosition = new AtomicLong();

            try (final FileChannel backupChannel = FileChannel.open(target.toPath(), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                writeLZ4EmptyFrame(backupChannel, hash32);

                writeLZ4FrameHeader(backupChannel, hash32, compressedFilePosition);
            }

            final int threadLimit = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
//          final long memoryLimit = Math.max((long) (Runtime.getRuntime().freeMemory() * 0.1), 64L * 1024 * 1024);
            final int compressorsLimit = threadLimit;//(int) Math.min(threadLimit, memoryLimit / (3 * LZ4_MAX_BLOCK_SIZE));

            if (logger.isInfoEnabled()) {
                logger.info("Amount of threads used for backup is set to " + threadLimit);
            }

            final ExecutorService streamMachinery =
                    Executors.newFixedThreadPool(compressorsLimit, r -> {
                        Thread thread = new Thread(r);
                        thread.setDaemon(true);

                        thread.setName("Parallel compressed backup thread");

                        return thread;
                    });

            final AtomicBoolean generatorStopped = new AtomicBoolean();
            final ConcurrentLinkedQueue<Pair<VirtualFileDescriptor, Long>> descriptors =
                    new ConcurrentLinkedQueue<>();
            final Semaphore queueSemaphore = new Semaphore(compressorsLimit * 1024);

            final ArrayList<Future<Void>> threads = new ArrayList<>();

            final ZipEncoding zipEncoding = ZipEncodingHelper.getZipEncoding(null);

            final LZ4Factory factory = LZ4Factory.safeInstance();
            final LZ4Compressor compressor = factory.fastCompressor();
            final int maxCompressedBlockSize = 2 * Integer.BYTES + compressor.maxCompressedLength(LZ4_MAX_BLOCK_SIZE);

            for (int i = 0; i < compressorsLimit; i++) {
                threads.add(streamMachinery.submit(() -> {
                    try (final FileChannel backupChannel = FileChannel.open(target.toPath(), StandardOpenOption.WRITE)) {
                        final ByteBuffer buffer = ByteBuffer.allocate(LZ4_MAX_BLOCK_SIZE);

                        final int compressionBufferSize = 2 * maxCompressedBlockSize;
                        final ByteBuffer compressedBuffer =
                                ByteBuffer.allocateDirect(compressionBufferSize).order(ByteOrder.LITTLE_ENDIAN);

                        while (true) {
                            boolean genStopped = generatorStopped.get();
                            Pair<VirtualFileDescriptor, Long> pair = descriptors.poll();

                            if (pair != null) {
                                queueSemaphore.release(1);
                                VirtualFileDescriptor fd = pair.first;
                                long fileSize = pair.second;

                                if (fd.hasContent()) {
                                    try (final InputStream fileStream = fd.getInputStream()) {
                                        long bytesWritten = 0;
                                        int fileIndex = 0;

                                        while (bytesWritten < fileSize) {
                                            final int chunkSize = (int) Math.min(buffer.remaining() -
                                                            TarConstants.DEFAULT_RCDSIZE,
                                                    fileSize - bytesWritten);

                                            if (chunkSize > 0) {
                                                final String fullPath;

                                                final String fdName = fd.getName();
                                                final int extensionIndex = fdName.lastIndexOf('.');

                                                if (extensionIndex >= 0 && extensionIndex < fdName.length() - 1) {
                                                    fullPath = String.format("%s%s-%08X%s", fd.getPath(),
                                                            fdName.substring(0, extensionIndex), fileIndex, fdName.substring(extensionIndex));
                                                } else {
                                                    fullPath = String.format("%s%s-%08X", fd.getPath(), fdName, fileIndex);
                                                }


                                                writeTarFileHeader(buffer, fullPath, chunkSize, fd.getTimeStamp(), zipEncoding);

                                                int bytesRead = 0;
                                                final byte[] bufferArray = buffer.array();
                                                int bufferOffset = buffer.arrayOffset();
                                                int bufferPosition = buffer.position();

                                                while (bytesRead < chunkSize) {
                                                    int r = fileStream.read(bufferArray, bufferOffset + bufferPosition,
                                                            chunkSize - bytesRead);

                                                    if (r == -1) {
                                                        break;
                                                    }

                                                    bufferPosition += r;
                                                    bytesRead += r;
                                                }

                                                if (bytesRead > 0) {
                                                    bytesWritten += bytesRead;
                                                    buffer.position(bufferPosition);
                                                } else {
                                                    throw new ExodusException("Invalid file size");
                                                }

                                                tarPadding(buffer);
                                                fileIndex++;
                                            } else {
                                                writeLZ4Block(buffer, compressedBuffer, compressor, hash32);

                                                if (compressedBuffer.remaining() < maxCompressedBlockSize) {
                                                    writeChunk(compressedBuffer, compressedFilePosition, backupChannel);
                                                }
                                            }
                                        }


                                    }
                                }
                            } else if (genStopped) {
                                break;
                            }
                        }

                        tarPadding(buffer);

                        writeLZ4Block(buffer, compressedBuffer, compressor, hash32);
                        writeChunk(compressedBuffer, compressedFilePosition, backupChannel);
                    }

                    return null;
                }));
            }


            for (final VirtualFileDescriptor fd : strategy.getContents()) {
                if (strategy.isInterrupted()) {
                    break;
                }

                if (fd.hasContent()) {
                    final long fileSize = Math.min(fd.getFileSize(), strategy.acceptFile(fd));
                    if (fileSize > 0) {
                        queueSemaphore.acquire();
                        descriptors.offer(new Pair<>(fd, fileSize));
                    }
                }
            }
            generatorStopped.set(true);

            try {
                for (Future<Void> thread : threads) {
                    thread.get();
                }
            } finally {
                streamMachinery.shutdown();
            }

            try (final FileChannel backupChannel = FileChannel.open(target.toPath(), StandardOpenOption.WRITE)) {
                final ByteBuffer lastBlock = ByteBuffer.allocate(2 * TarConstants.DEFAULT_RCDSIZE);
                final ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(2 * Integer.BYTES +
                        compressor.maxCompressedLength(lastBlock.remaining())).order(ByteOrder.LITTLE_ENDIAN);
                lastBlock.position(lastBlock.remaining());

                writeLZ4Block(lastBlock, compressedBuffer, compressor, hash32);
                writeChunk(compressedBuffer, compressedFilePosition, backupChannel);

                writeLZ4FrameEndMark(backupChannel);
            }

            if (strategy.isInterrupted()) {
                logger.info("Backup interrupted, deleting \"" + target.getName() + "\"...");
                IOUtil.deleteFile(target);
            } else {
                logger.info("Backup file \"" + target.getName() + "\" created.");
            }
        } catch (
                Throwable t) {
            strategy.onError(t);
            throw ExodusException.toExodusException(t, "Backup failed");
        } finally {
            strategy.afterBackup();
        }
        return target;
    }

    private static void tarPadding(ByteBuffer buffer) {
        final int reminder = TarConstants.DEFAULT_RCDSIZE -
                buffer.position() & (TarConstants.DEFAULT_RCDSIZE - 1);
        buffer.position(buffer.position() + reminder);
    }

    private static void writeTarFileHeader(final ByteBuffer buffer, final String path,
                                           final long size,
                                           final long ts,
                                           final ZipEncoding zipEncoding) throws IOException {
        final int arrayOffset = buffer.arrayOffset();
        final int position = buffer.position();
        final byte[] array = buffer.array();

        assert position % TarConstants.DEFAULT_RCDSIZE == 0;

        int offset = position + arrayOffset;

        //file path
        offset = TarUtils.formatNameBytes(normalizeTarFileName(path), array, offset, TarConstants.NAMELEN,
                zipEncoding);
        offset = TarUtils.formatLongOctalOrBinaryBytes(TarArchiveEntry.DEFAULT_FILE_MODE, array, offset,
                TarConstants.MODELEN);

        Arrays.fill(array, offset, offset + TarConstants.UIDLEN + TarConstants.GIDLEN, (byte) 0);
        //user id
        offset += TarConstants.UIDLEN;
        //group id
        offset += TarConstants.GIDLEN;

        offset = TarUtils.formatLongOctalOrBinaryBytes(size, array, offset, TarConstants.SIZELEN);
        //convert from java time to the tar time.
        offset = TarUtils.formatLongOctalOrBinaryBytes(ts / 1_000, array, offset, TarConstants.MODTIMELEN);

        final int csOffset = offset;
        Arrays.fill(array, offset, offset + TarConstants.CHKSUMLEN, (byte) ' ');
        offset += TarConstants.CHKSUMLEN;

        array[offset++] = TarConstants.LF_NORMAL;

        final long chk = computeTarCheckSum(array, arrayOffset + position, offset - (position + arrayOffset));

        TarUtils.formatCheckSumOctalBytes(chk, array, csOffset, TarConstants.CHKSUMLEN);

        Arrays.fill(array, offset, arrayOffset + position + TarConstants.DEFAULT_RCDSIZE, (byte) 0);

        buffer.position(position + TarConstants.DEFAULT_RCDSIZE);
    }

    private static long computeTarCheckSum(final byte[] buf, int off, int len) {
        long sum = 0;

        int end = off + len;
        for (int i = off; i < end; i++) {
            sum += (0xFF & buf[i]);
        }

        return sum;
    }

    private static void writeLZ4Block(final ByteBuffer buffer, final ByteBuffer compressionBuffer,
                                      final LZ4Compressor compressor, final XXHash32 hash32) {
        if (buffer.position() == 0) {
            return;
        }

        buffer.flip();

        final int startPosition = compressionBuffer.position();
        compressionBuffer.position(startPosition + Integer.BYTES);

        compressor.compress(buffer, compressionBuffer);
        final int endPosition = compressionBuffer.position();
        int len = endPosition - (startPosition + Integer.BYTES);

        boolean uncompressed = false;
        if (len > LZ4_MAX_BLOCK_SIZE) {
            compressionBuffer.position(startPosition + Integer.BYTES);

            buffer.flip();
            compressionBuffer.put(buffer);
            uncompressed = true;

            len = buffer.limit();
        }

        final int hash = hash32.hash(compressionBuffer, startPosition + Integer.BYTES, len, 0);
        compressionBuffer.putInt(hash);

        if (uncompressed) {
            compressionBuffer.putInt(startPosition, (1 << (Integer.SIZE - 1)) | len);
        } else {
            compressionBuffer.putInt(startPosition, len);
        }


        buffer.clear();
    }

    private static void writeLZ4FrameHeader(FileChannel channel, XXHash32 hash32, AtomicLong compressedPosition)
            throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN);
        writeLZ4FrameHeader(hash32, buffer);

        buffer.flip();

        compressedPosition.set(buffer.remaining());

        while (buffer.remaining() > 0) {
            channel.write(buffer);
        }
    }

    private static void writeLZ4FrameHeader(XXHash32 hash32, ByteBuffer buffer) {
        //magic number
        buffer.putInt(LZ4_MAGIC);

        //frame descriptor
        int position = buffer.position();
        buffer.put(LZ4_FLG);
        buffer.put(LZ4_BD_FLAG);
        int hash = hash32.hash(buffer, position, 2, 0);
        buffer.put((byte) ((hash >> 8) & 0xFF));
    }

    private static void writeLZ4FrameEndMark(FileChannel channel) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        writeLZ4FrameEndMark(buffer);
        buffer.flip();

        while (buffer.remaining() > 0) {
            channel.write(buffer);
        }
    }

    private static void writeLZ4FrameEndMark(ByteBuffer buffer) {
        buffer.putInt(LZ4_END_MARK);
    }

    private static void writeLZ4EmptyFrame(FileChannel channel, XXHash32 hash32) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.BYTES +
                3 * Byte.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        writeLZ4FrameHeader(hash32, buffer);
        writeLZ4FrameEndMark(buffer);

        while (buffer.remaining() > 0) {
            channel.write(buffer);
        }
    }

    private static void writeChunk(final ByteBuffer compressedBuffer,
                                   final AtomicLong compressedFilePosition,
                                   final FileChannel fileChannel) throws IOException {
        if (compressedBuffer.position() == 0) {
            return;
        }

        compressedBuffer.flip();

        long position;
        do {
            position = compressedFilePosition.get();
        } while (!compressedFilePosition.compareAndSet(position, position + compressedBuffer.limit()));


        fileChannel.position(position);

        while (compressedBuffer.remaining() > 0) {
            fileChannel.write(compressedBuffer);
        }

        compressedBuffer.clear();
    }

    private static String normalizeTarFileName(String fileName) {
        final String property = System.getProperty("os.name");
        if (property != null) {
            final String osName = property.toLowerCase(Locale.ROOT);

            if (osName.startsWith("windows")) {
                if (fileName.length() > 2) {
                    final char ch1 = fileName.charAt(0);
                    final char ch2 = fileName.charAt(1);

                    if (ch2 == ':' && (ch1 >= 'a' && ch1 <= 'z' || ch1 >= 'A' && ch1 <= 'Z')) {
                        fileName = fileName.substring(2);
                    }
                }
            } else if (osName.contains("netware")) {
                final int colon = fileName.indexOf(':');
                if (colon != -1) {
                    fileName = fileName.substring(colon + 1);
                }
            }
        }

        fileName = fileName.replace(File.separatorChar, '/');

        while (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        return fileName;
    }

    @NotNull
    public static String getTimeStampedTarGzFileName() {
        final StringBuilder builder = new StringBuilder(30);
        appendTimeStamp(builder);
        builder.append(".tar.gz");
        return builder.toString();
    }

    @NotNull
    public static String getTimeStampedTarLz4FileName() {
        final StringBuilder builder = new StringBuilder(30);
        appendTimeStamp(builder);
        builder.append(".tar.lz4");
        return builder.toString();
    }


    @NotNull
    public static String getTimeStampedZipFileName() {
        final StringBuilder builder = new StringBuilder(30);
        appendTimeStamp(builder);
        builder.append(".zip");
        return builder.toString();
    }

    public static void postProcessBackup(Path restoreDir) throws IOException {
        Files.walkFileTree(restoreDir, new FileVisitor<>() {
            private final HashMap<Path, TreeMap<Path, ArrayList<Path>>> filePeaces = new HashMap<>();

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                final String name = file.getFileName().toString();

                final int fileExtIndex = name.lastIndexOf(".");
                final int fileIndex;

                final String indexStr;
                final String fileName;
                if (fileExtIndex >= 0 && fileExtIndex < name.length() - 1) {
                    indexStr = name.substring(fileExtIndex - 8, fileExtIndex);
                    fileName = name.substring(0, fileExtIndex - 9) + name.substring(fileExtIndex);
                } else {
                    indexStr = name.substring(name.length() - 8);
                    fileName = name.substring(0, name.length() - 9);
                }
                fileIndex = Integer.parseInt(indexStr, 16);

                final Path absolutePath = file.toRealPath();
                final Path currentDirectory = absolutePath.getParent();
                final Path realFileName = currentDirectory.resolve(fileName);

                final TreeMap<Path, ArrayList<Path>> directoryFiles = filePeaces.computeIfAbsent(currentDirectory,
                        key -> new TreeMap<>());
                final ArrayList<Path> pieces = directoryFiles.computeIfAbsent(realFileName, k -> new ArrayList<>());
                while (pieces.size() <= fileIndex) {
                    pieces.add(null);
                }

                pieces.set(fileIndex, absolutePath);

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw new IOException(exc);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw new IOException(exc);
                }

                final TreeMap<Path, ArrayList<Path>> directoryFiles = filePeaces.remove(dir);
                if (directoryFiles != null) {
                    directoryFiles.forEach((fullFile, filePieces) -> {
                        try {
                            try {
                                Files.move(filePieces.get(0), fullFile, StandardCopyOption.ATOMIC_MOVE);
                            } catch (AtomicMoveNotSupportedException e) {
                                Files.move(filePieces.get(0), fullFile);
                            }

                            if (filePieces.size() == 1) {
                                return;
                            }

                            try (FileChannel channel =
                                         FileChannel.open(fullFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                                for (final Path piece : filePieces.subList(1, filePieces.size())) {
                                    long bytesWritten = 0;

                                    final long bytesToWrite = Files.size(piece);
                                    try (final FileChannel pieceChannel = FileChannel.open(piece, StandardOpenOption.READ)) {
                                        while (bytesWritten < bytesToWrite) {
                                            final long w = pieceChannel.transferTo(bytesWritten,
                                                    bytesToWrite - bytesWritten, channel);
                                            bytesWritten += w;
                                        }
                                    }
                                    Files.delete(piece);
                                }
                            }
                        } catch (IOException e) {
                            throw new ExodusException("Error during postprocessing of backup files.", e);
                        }
                    });
                }

                return FileVisitResult.CONTINUE;
            }
        });
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
    public static void tar(@NotNull File source, @NotNull File dest) throws IOException {
        if (!source.exists()) {
            throw new IllegalArgumentException("No source file or folder exists: " + source.getAbsolutePath());
        }
        if (dest.exists()) {
            throw new IllegalArgumentException("Destination refers to existing file or folder: " + dest.getAbsolutePath());
        }

        try (TarArchiveOutputStream tarOut = new TarArchiveOutputStream(new GZIPOutputStream(
                new BufferedOutputStream(Files.newOutputStream(dest.toPath())), 0x1000))) {
            doTar("", source, tarOut);
        } catch (IOException e) {
            IOUtil.deleteFile(dest); // operation filed, let's remove the destination archive
            throw e;
        }
    }

    private static void doTar(String pathInArchive,
                              File source,
                              TarArchiveOutputStream tarOut) throws IOException {
        if (source.isDirectory()) {
            for (File file : IOUtil.listFiles(source)) {
                doTar(pathInArchive + source.getName() + File.separator, file, tarOut);
            }
        } else {
            archiveFile(tarOut, new BackupStrategy.FileDescriptor(source, pathInArchive), source.length());
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
    public static void archiveFile(@NotNull final ArchiveOutputStream out,
                                   @NotNull final VirtualFileDescriptor source,
                                   final long fileSize) throws IOException {
        if (!source.hasContent()) {
            throw new IllegalArgumentException("Provided source is not a file: " + source.getPath());
        }
        //noinspection ChainOfInstanceofChecks
        if (out instanceof TarArchiveOutputStream) {
            final TarArchiveEntry entry = new TarArchiveEntry(source.getPath() + source.getName());
            entry.setSize(fileSize);
            entry.setModTime(source.getTimeStamp());
            out.putArchiveEntry(entry);
        } else if (out instanceof ZipArchiveOutputStream) {
            final ZipArchiveEntry entry = new ZipArchiveEntry(source.getPath() + source.getName());
            entry.setSize(fileSize);
            entry.setTime(source.getTimeStamp());
            out.putArchiveEntry(entry);
        } else {
            throw new IOException("Unknown archive output stream");
        }
        final InputStream input = source.getInputStream();
        try {
            IOUtil.copyStreams(input, fileSize, out, IOUtil.getBUFFER_ALLOCATOR());
        } finally {
            if (source.shouldCloseStream()) {
                input.close();
            }
        }
        out.closeArchiveEntry();
    }

    @SuppressWarnings("unused")
    public static void archiveFile(@NotNull final ArchiveOutputStream out,
                                   CompressionEntry compressionEntry) throws IOException {
        //noinspection ChainOfInstanceofChecks
        if (out instanceof TarArchiveOutputStream) {
            final TarArchiveEntry entry = new TarArchiveEntry(compressionEntry.path + compressionEntry.name);
            entry.setSize(compressionEntry.fileSize);
            entry.setModTime(compressionEntry.timeStamp);
            out.putArchiveEntry(entry);
        } else if (out instanceof ZipArchiveOutputStream) {
            final ZipArchiveEntry entry = new ZipArchiveEntry(compressionEntry.path + compressionEntry.name);
            entry.setSize(compressionEntry.fileSize);
            entry.setTime(compressionEntry.timeStamp);
            out.putArchiveEntry(entry);
        } else {
            throw new IOException("Unknown archive output stream");
        }
        try {
            IOUtil.copyStreams(compressionEntry.input, compressionEntry.fileSize, out, IOUtil.getBUFFER_ALLOCATOR());
        } finally {
            if (compressionEntry.shouldCloseStream) {
                compressionEntry.input.close();
            }
        }
        out.closeArchiveEntry();
    }

    private static void appendTimeStamp(final StringBuilder builder) {
        final Calendar now = Calendar.getInstance();
        builder.append(now.get(Calendar.YEAR));
        builder.append('-');
        appendZeroPadded(builder, now.get(Calendar.MONTH) + 1);
        builder.append('-');
        appendZeroPadded(builder, now.get(Calendar.DAY_OF_MONTH));
        builder.append('-');
        appendZeroPadded(builder, now.get(Calendar.HOUR_OF_DAY));
        builder.append('-');
        appendZeroPadded(builder, now.get(Calendar.MINUTE));
        builder.append('-');
        appendZeroPadded(builder, now.get(Calendar.SECOND));
    }

    private static void appendZeroPadded(final StringBuilder builder, int value) {
        if (value < 10) {
            builder.append('0');
        }
        builder.append(value);
    }

    private static final class CompressionEntry {
        final InputStream input;
        final String path;
        final String name;
        final long timeStamp;
        final long fileSize;
        final boolean shouldCloseStream;

        private CompressionEntry(InputStream input, String path, String name, long timeStamp, long fileSize, boolean shouldCloseStream) {
            this.input = input;
            this.path = path;
            this.name = name;
            this.timeStamp = timeStamp;
            this.fileSize = fileSize;
            this.shouldCloseStream = shouldCloseStream;
        }
    }

}
