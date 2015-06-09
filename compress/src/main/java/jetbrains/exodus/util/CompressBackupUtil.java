/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.util;

import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.Backupable;
import jetbrains.exodus.ExodusException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Calendar;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class CompressBackupUtil {

    private static final Logger logger = LoggerFactory.getLogger(CompressBackupUtil.class);

    private CompressBackupUtil() {
    }

    @NotNull
    public static File backup(@NotNull final Backupable target, @NotNull final File backupRoot,
                              @Nullable final String backupNamePrefix, final boolean zip) throws Exception {
        if (!backupRoot.exists() && !backupRoot.mkdirs()) {
            throw new IOException("Failed to create " + backupRoot.getAbsolutePath());
        }
        final File backupFile;
        final BackupStrategy strategy = target.getBackupStrategy();
        strategy.beforeBackup();
        try {
            final ArchiveOutputStream archive;
            if (zip) {
                final String fileName = getTimeStampedZipFileName();
                backupFile = new File(backupRoot, backupNamePrefix == null ? fileName : backupNamePrefix + fileName);
                final ZipArchiveOutputStream zipArchive =
                        new ZipArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(backupFile)));
                zipArchive.setLevel(Deflater.BEST_COMPRESSION);
                archive = zipArchive;
            } else {
                final String fileName = getTimeStampedTarGzFileName();
                backupFile = new File(backupRoot, backupNamePrefix == null ? fileName : backupNamePrefix + fileName);
                archive = new TarArchiveOutputStream(new GZIPOutputStream(
                        new BufferedOutputStream(new FileOutputStream(backupFile)), 0x1000));
            }
            for (final BackupStrategy.FileDescriptor fd : strategy.listFiles()) {
                final File file = fd.getFile();
                if (file.isFile()) {
                    final long fileSize = Math.min(fd.getFileSize(), strategy.acceptFile(file));
                    if (fileSize > 0L) {
                        archiveFile(archive, fd.getPath(), file, fileSize);
                    }
                }
            }
            archive.close();
            logger.info("Backup file \"" + backupFile.getName() + "\" created.");
        } catch (Throwable t) {
            strategy.onError(t);
            throw ExodusException.toExodusException(t, "Backup failed");
        } finally {
            strategy.afterBackup();
        }
        return backupFile;
    }

    @NotNull
    public static String getTimeStampedTarGzFileName() {
        final StringBuilder builder = new StringBuilder(30);
        appendTimeStamp(builder);
        builder.append(".tar.gz");
        return builder.toString();
    }

    @NotNull
    public static String getTimeStampedZipFileName() {
        final StringBuilder builder = new StringBuilder(30);
        appendTimeStamp(builder);
        builder.append(".zip");
        return builder.toString();
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

        TarArchiveOutputStream tarOut = null;
        try {
            tarOut = new TarArchiveOutputStream(new GZIPOutputStream(
                    new BufferedOutputStream(new FileOutputStream(dest)), 0x1000));
            doTar("", source, tarOut);
            tarOut.close();
        } catch (IOException e) {
            cleanUp(tarOut, dest); // operation filed, let's remove the destination archive
            throw e;
        }
    }

    private static void cleanUp(TarArchiveOutputStream tarOut, File dest) {
        if (tarOut != null) {
            try {
                tarOut.close();
            } catch (IOException e) {
                // nothing to do here
            }
        }
        IOUtil.deleteFile(dest);
    }

    private static void doTar(String pathInArchive,
                              File source,
                              TarArchiveOutputStream tarOut) throws IOException {
        if (source.isDirectory()) {
            for (File file : IOUtil.listFiles(source)) {
                doTar(pathInArchive + source.getName() + File.separator, file, tarOut);
            }
        } else {
            archiveFile(tarOut, pathInArchive, source, source.length());
        }
    }

    /**
     * Adds the file to the tar archive represented by output stream. It's caller's responsibility to close output stream
     * properly.
     *
     * @param out           target archive.
     * @param pathInArchive relative path in archive. It will lead the name of the file in the archive.
     * @param source        file to be added.
     * @param fileSize      size of the file (which is known in most cases).
     * @throws IOException in case of any issues with underlying store.
     */
    public static void archiveFile(@NotNull final ArchiveOutputStream out,
                                   @NotNull final String pathInArchive,
                                   @NotNull final File source,
                                   final long fileSize) throws IOException {
        if (!source.isFile()) {
            throw new IllegalArgumentException("Provided source is not a file: " + source.getAbsolutePath());
        }
        //noinspection ChainOfInstanceofChecks
        if (out instanceof TarArchiveOutputStream) {
            final TarArchiveEntry entry = new TarArchiveEntry(pathInArchive + source.getName());
            entry.setSize(fileSize);
            entry.setModTime(source.lastModified());
            out.putArchiveEntry(entry);
        } else if (out instanceof ZipArchiveOutputStream) {
            final ZipArchiveEntry entry = new ZipArchiveEntry(pathInArchive + source.getName());
            entry.setSize(fileSize);
            entry.setTime(source.lastModified());
            out.putArchiveEntry(entry);
        } else {
            throw new IOException("Unknown archive output stream");
        }
        try (InputStream input = new FileInputStream(source)) {
            IOUtil.copyStreams(input, fileSize, out, IOUtil.BUFFER_ALLOCATOR);
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
}
