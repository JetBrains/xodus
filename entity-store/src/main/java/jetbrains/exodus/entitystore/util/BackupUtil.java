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
package jetbrains.exodus.entitystore.util;

import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.crypto.*;
import jetbrains.exodus.entitystore.FileSystemBlobVault;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.BufferedDataWriter;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.StartupMetadata;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.concurrent.Executors;
import java.util.zip.Deflater;


public class BackupUtil {
    private static final Logger logger = LoggerFactory.getLogger(BackupUtil.class);

    public static InputStream reEncryptBackup(final ZipFile zipFile,
                                              final byte[] firstKey, final long firstIv,
                                              final byte[] secondKey, final long secondIv,
                                              final StreamCipherProvider cipherProvider)
            throws IOException {
        var zipStream = new ZipFileArchiveInputStream(zipFile);
        return reEncryptBackup(zipStream, firstKey, firstIv, secondKey, secondIv, cipherProvider);
    }


    public static InputStream reEncryptBackup(final ArchiveInputStream archiveStream,
                                              final byte[] firstKey, final long firstIv,
                                              final byte[] secondKey, final long secondIv,
                                              final StreamCipherProvider cipherProvider)
            throws IOException {
        var pipedInputStream = new PipedInputStream(1024 * 1024);
        var pipedOutputStream = new PipedOutputStream(pipedInputStream);

        var errorAwareInputStream = new ErrorAwareInputStream(pipedInputStream);
        var bufferedOutputStream = new BufferedOutputStream(pipedOutputStream, 1024 * 1024);
        final TarArchiveOutputStream tarArchiveOutputStream;
        if (secondKey == null) {
            tarArchiveOutputStream = new TarArchiveOutputStream(new GzipCompressorOutputStream(bufferedOutputStream));
        } else {
            var gzipParameters = new GzipParameters();
            gzipParameters.setCompressionLevel(Deflater.NO_COMPRESSION);

            tarArchiveOutputStream =
                    new TarArchiveOutputStream(new GzipCompressorOutputStream(bufferedOutputStream, gzipParameters));
        }

        var executor = Executors.newSingleThreadExecutor((runnable) -> {
            var thread = new Thread(runnable);
            thread.setName("Backup re-encryption thread");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception in thread " + t.getName(), e));
            return thread;
        });

        executor.submit(() -> {
            try {
                final byte[] readBuffer = new byte[1024 * 1024];

                int binaryFormatVersion = -1;
                int pageSize = -1;
                long fileLengthBound = -1;

                final int pageStep = 4 * 1024;
                final int pageMaxSize = 256 * 1024;

                var inputEntry = archiveStream.getNextEntry();
                while (inputEntry != null) {
                    //skip directories we are interested only in files
                    if (inputEntry.isDirectory()) {
                        inputEntry = archiveStream.getNextEntry();
                        continue;
                    }
                    var name = inputEntry.getName();

                    var outputArchiveEntry = new TarArchiveEntry(name);
                    var entrySize = inputEntry.getSize();

                    if (entrySize < 0) {
                        throw new IllegalStateException("Archive entries with unknown size are not" +
                                " supported. Entry " + name + " should provide size to be re-encrypted");
                    }

                    outputArchiveEntry.setSize(entrySize);
                    outputArchiveEntry.setModTime(inputEntry.getLastModifiedDate());

                    tarArchiveOutputStream.putArchiveEntry(outputArchiveEntry);


                    if (name.endsWith(LogUtil.LOG_FILE_EXTENSION)) {
                        int processed = 0;
                        int readBufferOffset = 0;

                        final Path namePath = Path.of(name);
                        final long fileAddress = LogUtil.getAddress(namePath.getFileName().toString());

                        while (processed < entrySize) {
                            if (binaryFormatVersion == EnvironmentImpl.CURRENT_FORMAT_VERSION) {
                                if (fileLengthBound >= 0 && entrySize > fileLengthBound) {
                                    throw new IllegalStateException("Backup is broken, size of the file " + name +
                                            " should not exceed " + fileLengthBound);
                                }

                                if ((entrySize & (pageSize - 1)) != 0) {
                                    throw new IllegalStateException("Backup is broken, size of the file " + name +
                                            " should be quantified by " + pageSize + " size of the file is "
                                            + fileLengthBound);
                                }
                            }

                            final int readSize = (int) Math.min(entrySize - processed, readBuffer.length - readBufferOffset);
                            assert binaryFormatVersion < EnvironmentImpl.CURRENT_FORMAT_VERSION ||
                                    ((readSize + readBufferOffset) & (pageSize - 1)) == 0;


                            IOUtils.readFully(archiveStream, readBuffer, readBufferOffset, readSize);
                            final int bufferSize = readSize + readBufferOffset;

                            readBufferOffset = 0;

                            if (binaryFormatVersion < 0) {
                                var versionInformation = detectFormatVersion(
                                        readBuffer, bufferSize, pageStep, pageMaxSize);

                                binaryFormatVersion = versionInformation[0];
                                pageSize = versionInformation[1];

                                assert binaryFormatVersion >= 0;
                            }

                            final byte[] dataToWrite;
                            final int dataToWriteLen;

                            if (binaryFormatVersion < EnvironmentImpl.CURRENT_FORMAT_VERSION) {
                                dataToWrite = readBuffer;
                                dataToWriteLen = bufferSize;

                                if (firstKey != null) {
                                    assert cipherProvider != null;

                                    EnvKryptKt.cryptBlocksMutable(cipherProvider, firstKey, firstIv,
                                            fileAddress + processed, readBuffer, 0, bufferSize,
                                            LogUtil.LOG_BLOCK_ALIGNMENT);
                                }

                                if (secondKey != null) {
                                    assert cipherProvider != null;

                                    EnvKryptKt.cryptBlocksMutable(cipherProvider, secondKey, secondIv,
                                            fileAddress + processed, readBuffer, 0, bufferSize,
                                            LogUtil.LOG_BLOCK_ALIGNMENT);
                                }
                            } else {
                                var pages = bufferSize / pageSize;
                                dataToWriteLen = pages * pageSize;

                                dataToWrite = new byte[dataToWriteLen];

                                if (firstKey != null) {
                                    assert cipherProvider != null;

                                    encryptV2FormatPages(firstKey, firstIv, cipherProvider,
                                            readBuffer, pageSize, processed, fileAddress, dataToWrite, dataToWriteLen);
                                }

                                for (int dataOffset = 0; dataOffset < dataToWriteLen; dataOffset += pageSize) {
                                    final long calculatedHash = BufferedDataWriter.calculateHash(dataToWrite,
                                            dataOffset, pageSize - BufferedDataWriter.HASH_CODE_SIZE);
                                    final long storedHash = BindingUtils.readLong(dataToWrite, dataOffset +
                                            pageSize - BufferedDataWriter.HASH_CODE_SIZE);

                                    if (storedHash != calculatedHash) {
                                        throw new IllegalStateException(
                                                "Page is broken. Expected and calculated hash codes are different." +
                                                        " File name " + name + ". Page address " +
                                                        (processed + dataOffset));
                                    }
                                }

                                if (secondKey != null) {
                                    assert cipherProvider != null;

                                    encryptV2FormatPages(secondKey, secondIv, cipherProvider,
                                            readBuffer, pageSize, processed, fileAddress, dataToWrite, dataToWriteLen);
                                }

                                readBufferOffset = bufferSize - dataToWriteLen;
                                if (readBufferOffset > 0) {
                                    System.arraycopy(readBuffer, bufferSize - readBufferOffset, readBuffer, 0, readBufferOffset);
                                }
                            }

                            tarArchiveOutputStream.write(dataToWrite, 0, dataToWriteLen);
                            processed += dataToWriteLen;
                        }
                    } else if (name.endsWith(PersistentEntityStoreImpl.BLOBS_EXTENSION)) {
                        final InputStream entryInputStream;
                        final OutputStream entryOutputStream;

                        final File blobFile = new File(name);
                        final File vault = findBlobsVault(blobFile);

                        long blobHandle = FileSystemBlobVault.getBlobHandleByFile(blobFile,
                                PersistentEntityStoreImpl.BLOBS_EXTENSION, vault);

                        if (firstKey != null) {
                            assert cipherProvider != null;

                            var cipher = EncryptedBlobVault.newCipher(cipherProvider, blobHandle, firstKey, firstIv);
                            entryInputStream = new StreamCipherInputStream(archiveStream, () -> cipher);
                        } else {
                            entryInputStream = archiveStream;
                        }

                        if (secondKey != null) {
                            assert cipherProvider != null;

                            var cipher = EncryptedBlobVault.newCipher(cipherProvider, blobHandle, secondKey, secondIv);
                            entryOutputStream = new StreamCipherOutputStream(tarArchiveOutputStream, cipher);
                        } else {
                            entryOutputStream = tarArchiveOutputStream;
                        }

                        IOUtils.copyLarge(entryInputStream, entryOutputStream, readBuffer);
                    } else if (name.equals(StartupMetadata.FIRST_FILE_NAME) ||
                            name.endsWith("/" + StartupMetadata.FIRST_FILE_NAME) || name.equals(StartupMetadata.SECOND_FILE_NAME)
                            || name.endsWith("/" + StartupMetadata.SECOND_FILE_NAME)) {

                        if (binaryFormatVersion >= 0 && binaryFormatVersion < EnvironmentImpl.CURRENT_FORMAT_VERSION) {
                            throw new IllegalStateException("Backup is broken please fix backup consistency by " +
                                    "opening it in database and correctly closing database. " +
                                    "Database will restore data automatically.");
                        }

                        IOUtils.readFully(archiveStream, readBuffer, 0, StartupMetadata.FILE_SIZE);
                        var buffer = ByteBuffer.wrap(readBuffer);
                        buffer.limit(StartupMetadata.FILE_SIZE);

                        if (StartupMetadata.getFileVersion(buffer) >= 0) {
                            var startupMetadata = StartupMetadata.deserialize(buffer, 0, false);
                            binaryFormatVersion = EnvironmentImpl.CURRENT_FORMAT_VERSION;

                            if (pageSize > 0 && pageSize != startupMetadata.getPageSize()) {
                                throw new IllegalStateException("Backup is broken please fix backup consistency by " +
                                        "opening it in database and correctly closing database. " +
                                        "Database will restore data automatically.");
                            }

                            pageSize = startupMetadata.getPageSize();
                            fileLengthBound = startupMetadata.getFileLengthBoundary();
                        }

                        tarArchiveOutputStream.write(readBuffer, 0, StartupMetadata.FILE_SIZE);
                    } else {
                        IOUtils.copyLarge(archiveStream, tarArchiveOutputStream, readBuffer);
                    }

                    tarArchiveOutputStream.closeArchiveEntry();
                    inputEntry = archiveStream.getNextEntry();
                }
            } catch (final Throwable e) {
                errorAwareInputStream.error = e;
                logger.error("Error during backup re-encryption", e);
            } finally {
                try {
                    archiveStream.close();
                } catch (IOException e) {
                    errorAwareInputStream.error = e;
                    logger.error("Error during backup re-encryption", e);
                }

                try {
                    tarArchiveOutputStream.close();
                } catch (IOException e) {
                    errorAwareInputStream.error = e;
                    logger.error("Error during backup re-encryption", e);
                }
            }
        });

        return errorAwareInputStream;
    }

    private static void encryptV2FormatPages(byte[] key, long iv,
                                             StreamCipherProvider cipherProvider,
                                             byte[] readBuffer, int pageSize, int processed, long fileAddress,
                                             byte[] dataToWrite, int dataToWriteLen) {
        for (int dataOffset = 0; dataOffset < dataToWriteLen; dataOffset += pageSize) {
            EnvKryptKt.cryptBlocksMutable(cipherProvider, key, iv,
                    fileAddress + processed + dataOffset, readBuffer, dataOffset,
                    pageSize - BufferedDataWriter.HASH_CODE_SIZE,
                    LogUtil.LOG_BLOCK_ALIGNMENT);
            System.arraycopy(readBuffer, dataOffset, dataToWrite, dataOffset, pageSize);
        }

    }

    private static File findBlobsVault(File file) {

        while (true) {
            File parent = file.getParentFile();
            if (parent == null) {
                break;
            }

            if (parent.getName().equals(PersistentEntityStoreImpl.BLOBS_DIR)) {
                return parent;
            }

            file = parent;
        }

        throw new IllegalStateException("Can not find blob vault root for file " + file);
    }

    @SuppressWarnings("SameParameterValue")
    private static int[] detectFormatVersion(final byte[] buffer,
                                             final int bufferSize, final int pageStep,
                                             final int pageMaxSize) {
        for (int pageSize = pageStep; pageSize <= pageMaxSize && pageSize <= bufferSize; pageSize += pageStep) {
            final long storedHash = BindingUtils.readLong(buffer,
                    pageSize - BufferedDataWriter.HASH_CODE_SIZE);
            final long calculatedHash = BufferedDataWriter.calculateHash(buffer, 0,
                    pageSize - BufferedDataWriter.HASH_CODE_SIZE);

            if (storedHash == calculatedHash) {
                return new int[]{EnvironmentImpl.CURRENT_FORMAT_VERSION, pageSize};
            }
        }

        return new int[]{1, -1};
    }

    private static final class ErrorAwareInputStream extends FilterInputStream {
        volatile Throwable error;

        ErrorAwareInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            checkError();
            return super.read();
        }

        private void checkError() throws IOException {
            if (error != null) {
                throw new IOException(error.getMessage(), error);
            }
        }

        @Override
        public int read(@NotNull byte[] b) throws IOException {
            checkError();
            return super.read(b);
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) throws IOException {
            checkError();
            return super.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            checkError();
            return super.skip(n);
        }

        @Override
        public void close() throws IOException {
            super.close();
            checkError();
        }
    }

    private static class ZipFileArchiveInputStream extends ArchiveInputStream {
        private final ZipFile zipFile;
        private final Enumeration<ZipArchiveEntry> entries;
        private InputStream currentEntryInputStream;

        public ZipFileArchiveInputStream(ZipFile zipFile) {
            this.zipFile = zipFile;
            this.entries = zipFile.getEntries();
        }

        @Override
        public ArchiveEntry getNextEntry() throws IOException {
            if (!entries.hasMoreElements()) {
                return null;
            }

            closeCurrentEntryStream();
            org.apache.commons.compress.archivers.zip.ZipArchiveEntry entry = entries.nextElement();
            currentEntryInputStream = zipFile.getInputStream(entry);
            return entry;
        }

        @Override
        public int read() throws IOException {
            if (currentEntryInputStream == null) {
                throw new IllegalStateException("No entry is open for reading");
            }
            return currentEntryInputStream.read();
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) throws IOException {
            if (currentEntryInputStream == null) {
                throw new IllegalStateException("No entry is open for reading");
            }
            return currentEntryInputStream.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            closeCurrentEntryStream();
            zipFile.close();
        }

        private void closeCurrentEntryStream() throws IOException {
            if (currentEntryInputStream != null) {
                currentEntryInputStream.close();
                currentEntryInputStream = null;
            }
        }
    }

}
