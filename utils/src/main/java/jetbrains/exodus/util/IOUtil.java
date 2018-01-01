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

import org.jetbrains.annotations.NotNull;

import java.io.*;

/**
 * Encapsulates utility methods for IO and compression routine.
 */
public class IOUtil {

    public static final int READ_BUFFER_SIZE = 0x4000;
    public static final ByteArraySpinAllocator BUFFER_ALLOCATOR = new ByteArraySpinAllocator(READ_BUFFER_SIZE);

    private static final String BLOCK_SIZE = "exodus.io.blockSize";
    private static final File[] NO_FILES = new File[0];

    private IOUtil() {
    }

    public static long getBlockSize() {
        return Long.getLong(BLOCK_SIZE, 0x1000);
    }

    public static long getAdjustedFileLength(File file) {
        final long blockSize = getBlockSize();
        return (file.length() + blockSize - 1) / blockSize * blockSize;
    }

    public static long getDirectorySize(File dir, final String extension, boolean recursive) {
        long sum = 0L;

        if (recursive) {

            for (File childDir : listFiles(dir, new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isDirectory();
                }
            })) {
                sum += getDirectorySize(childDir, extension, /*always true*/recursive);
            }
        }

        for (final File file : listFiles(dir, new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(extension);
            }
        })) {
            sum += getAdjustedFileLength(file);
        }

        return sum;
    }

    public static void copyStreams(@NotNull final InputStream source,
                                   @NotNull final OutputStream target,
                                   @NotNull final ByteArraySpinAllocator bufferAllocator) throws IOException {
        copyStreams(source, Long.MAX_VALUE, target, bufferAllocator);
    }

    public static void copyStreams(@NotNull final InputStream source,
                                   final long sourceLen,
                                   @NotNull final OutputStream target,
                                   @NotNull final ByteArraySpinAllocator bufferAllocator) throws IOException {
        final byte[] buffer = bufferAllocator.alloc();
        try {
            long totalRead = 0;
            int read;
            while (totalRead < sourceLen && (read = source.read(buffer)) >= 0) {
                if (read > 0) {
                    read = (int) Math.min(sourceLen - totalRead, read);
                    target.write(buffer, 0, read);
                    totalRead += read;
                }
            }
        } finally {
            bufferAllocator.dispose(buffer);
        }
    }

    public static void deleteRecursively(final File dir) {
        for (final File file : listFiles(dir)) {
            if (file.isDirectory()) {
                deleteRecursively(file);
            }
            deleteFile(file);
        }
    }

    public static void deleteFile(File file) {
        if (!file.delete()) {
            file.deleteOnExit();
        }
    }

    @NotNull
    public static File[] listFiles(@NotNull final File directory) {
        final File[] result = directory.listFiles();
        return result == null ? NO_FILES : result;
    }

    @NotNull
    public static File[] listFiles(@NotNull final File directory, @NotNull final FilenameFilter filter) {
        final File[] result = directory.listFiles(filter);
        return result == null ? NO_FILES : result;
    }

    @NotNull
    public static File[] listFiles(@NotNull final File directory, @NotNull final FileFilter filter) {
        final File[] result = directory.listFiles(filter);
        return result == null ? NO_FILES : result;
    }

    public static int readFully(@NotNull final InputStream input, @NotNull final byte[] bytes) throws IOException {
        return readFully(input, bytes, bytes.length);
    }

    public static int readFully(@NotNull final InputStream input, @NotNull final byte[] bytes, final int len) throws IOException {
        int off = 0;
        while (off < len) {
            final int read = input.read(bytes, off, len - off);
            if (read < 0) {
                break;
            }
            off += read;
        }
        return off;
    }
}
