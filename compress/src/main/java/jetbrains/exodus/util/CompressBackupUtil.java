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
package jetbrains.exodus.util;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.backup.BackupBean;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.Backupable;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.env.Environment;
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
     *
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
        try {
            final ArchiveOutputStream archive;
            if (zip) {
                final ZipArchiveOutputStream zipArchive =
                        new ZipArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(target)));
                zipArchive.setLevel(Deflater.BEST_COMPRESSION);
                archive = zipArchive;
            } else {
                archive = new TarArchiveOutputStream(new GZIPOutputStream(
                        new BufferedOutputStream(new FileOutputStream(target))));
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
            IOUtil.copyStreams(input, fileSize, out, IOUtil.BUFFER_ALLOCATOR);
        } finally {
            if (source.shouldCloseStream()) {
                input.close();
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
}
