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
package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.util.IOUtil;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class StartupMetadata {
    private static final int HASHCODE_OFFSET = 0;
    private static final int HASH_CODE_SIZE = Long.BYTES;

    private static final int FILE_VERSION_OFFSET = HASHCODE_OFFSET + HASH_CODE_SIZE;
    private static final int FILE_VERSION_BYTES = Long.BYTES;

    private static final int FORMAT_VERSION_OFFSET = FILE_VERSION_OFFSET + FILE_VERSION_BYTES;
    private static final int FORMAT_VERSION_BYTES = Integer.BYTES;

    private static final int ENVIRONMENT_FORMAT_VERSION_OFFSET = FORMAT_VERSION_OFFSET + FORMAT_VERSION_BYTES;
    private static final int ENVIRONMENT_FORMAT_VERSION_BYTES = Integer.BYTES;

    private static final int DB_ROOT_ADDRESS_OFFSET = ENVIRONMENT_FORMAT_VERSION_OFFSET + ENVIRONMENT_FORMAT_VERSION_BYTES;
    private static final int DB_ROOT_BYTES = Long.BYTES;

    private static final int PAGE_SIZE_OFFSET = DB_ROOT_ADDRESS_OFFSET + DB_ROOT_BYTES;
    private static final int PAGE_SIZE_BYTES = Integer.BYTES;

    private static final int FILE_LENGTH_BOUNDARY_OFFSET = PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES;
    private static final int FILE_LENGTH_BOUNDARY_BYTES = Long.BYTES;

    private static final int CORRECTLY_CLOSED_FLAG_OFFSET = FILE_LENGTH_BOUNDARY_OFFSET + FILE_LENGTH_BOUNDARY_BYTES;
    public static final int CLOSED_FLAG_BYTES = Byte.BYTES;

    private static final int FILE_SIZE = CORRECTLY_CLOSED_FLAG_OFFSET + CLOSED_FLAG_BYTES;

    public static final String FIRST_FILE_NAME = "startup-metadata-0";
    public static final String SECOND_FILE_NAME = "startup-metadata-1";


    static final int FORMAT_VERSION = 1;

    private final boolean useFirstFile;

    private volatile long rootAddress;
    private final boolean isCorrectlyClosed;
    private final int pageSize;
    private final long currentVersion;

    private final int environmentFormatVersion;
    private final long fileLengthBoundary;

    private StartupMetadata(final boolean useFirstFile, final long rootAddress,
                            final boolean isCorrectlyClosed, int pageSize, long currentVersion,
                            int environmentFormatVersion,
                            long fileLengthBoundary) {
        this.useFirstFile = useFirstFile;
        this.rootAddress = rootAddress;
        this.isCorrectlyClosed = isCorrectlyClosed;
        this.pageSize = pageSize;
        this.currentVersion = currentVersion;
        this.environmentFormatVersion = environmentFormatVersion;
        this.fileLengthBoundary = fileLengthBoundary;
    }

    public int getEnvironmentFormatVersion() {
        return environmentFormatVersion;
    }

    public long getFileLengthBoundary() {
        return fileLengthBoundary;
    }

    public long getRootAddress() {
        return rootAddress;
    }

    public void setRootAddress(long rootAddress) {
        this.rootAddress = rootAddress;
    }

    public boolean isCorrectlyClosed() {
        return isCorrectlyClosed;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void closeAndUpdate(final FileDataReader reader) throws IOException {
        final Path dbPath = Paths.get(reader.getLocation());
        final ByteBuffer content = serialize(currentVersion, environmentFormatVersion, rootAddress,
                pageSize, fileLengthBoundary, true);
        store(content, dbPath, useFirstFile);
    }

    public static @Nullable StartupMetadata open(final FileDataReader reader,
                                                 final boolean isReadOnly, final int pageSize,
                                                 final int environmentFormatVersion,
                                                 final long fileLengthBoundary) throws IOException {
        final Path dbPath = Paths.get(reader.getLocation());
        final Path firstFilePath = dbPath.resolve(FIRST_FILE_NAME);
        final Path secondFilePath = dbPath.resolve(SECOND_FILE_NAME);

        long firstFileVersion;
        long secondFileVersion;

        final ByteBuffer firstFileContent;
        final ByteBuffer secondFileContent;

        @SuppressWarnings("DuplicatedCode") final boolean firstFileExist = Files.exists(firstFilePath);

        if (firstFileExist) {
            try (final FileChannel channel = FileChannel.open(firstFilePath, StandardOpenOption.READ)) {
                firstFileContent = IOUtil.readFully(channel);
            }

            firstFileVersion = getFileVersion(firstFileContent);
        } else {
            firstFileVersion = -1;
            firstFileContent = null;
        }

        final boolean secondFileExist = Files.exists(secondFilePath);
        if (secondFileExist) {
            try (final FileChannel channel = FileChannel.open(secondFilePath, StandardOpenOption.READ)) {
                secondFileContent = IOUtil.readFully(channel);
            }

            secondFileVersion = getFileVersion(secondFileContent);
        } else {
            secondFileVersion = -1;
            secondFileContent = null;
        }

        if (firstFileVersion < 0 && firstFileExist && !isReadOnly) {
            Files.deleteIfExists(firstFilePath);
        }

        if (secondFileVersion < 0 && secondFileExist && !isReadOnly) {
            Files.deleteIfExists(secondFilePath);
        }

        final ByteBuffer content;
        final long nextVersion;
        final boolean useFirstFile;

        if (firstFileVersion < secondFileVersion) {
            if (firstFileExist && !isReadOnly) {
                Files.deleteIfExists(firstFilePath);
            }

            nextVersion = secondFileVersion + 1;
            content = secondFileContent;
            useFirstFile = true;
        } else if (secondFileVersion < firstFileVersion) {
            if (secondFileExist && !isReadOnly) {
                Files.deleteIfExists(secondFilePath);
            }

            nextVersion = firstFileVersion + 1;
            content = firstFileContent;
            useFirstFile = false;
        } else {
            content = null;
            nextVersion = 0;
            useFirstFile = true;
        }

        if (content == null) {
            final ByteBuffer updatedMetadata = serialize(1, environmentFormatVersion, -1,
                    pageSize, fileLengthBoundary, false);
            store(updatedMetadata, dbPath, useFirstFile);

            return null;
        }

        final StartupMetadata result = deserialize(content, nextVersion + 1, !useFirstFile);

        if (!isReadOnly) {
            final ByteBuffer updatedMetadata = serialize(nextVersion, result.environmentFormatVersion, -1,
                    result.pageSize, result.fileLengthBoundary, false);
            store(updatedMetadata, dbPath, useFirstFile);
        }

        return result;
    }

    public static StartupMetadata createStub(int pageSize, final int environmentFormatVersion, final long fileLengthBoundary) {
        return new StartupMetadata(false, -1, true, pageSize, 1,
                environmentFormatVersion, fileLengthBoundary);
    }


    private static void store(ByteBuffer content, final Path dbPath, final boolean useFirstFile) throws IOException {
        final Path filePath;

        if (useFirstFile) {
            filePath = dbPath.resolve(FIRST_FILE_NAME);
        } else {
            filePath = dbPath.resolve(SECOND_FILE_NAME);
        }

        try (final FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {
            channel.write(content);
            channel.force(true);
        }

        if (useFirstFile) {
            Files.deleteIfExists(dbPath.resolve(SECOND_FILE_NAME));
        } else {
            Files.deleteIfExists(dbPath.resolve(FIRST_FILE_NAME));
        }
    }

    private static long getFileVersion(ByteBuffer content) {
        if (content.remaining() != FILE_SIZE) {
            return -1;
        }

        final long hash = BufferedDataWriter.xxHash.hash(content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED);

        if (hash != content.getLong(HASHCODE_OFFSET)) {
            return -1;
        }

        return content.getInt(FORMAT_VERSION_OFFSET);
    }

    private static StartupMetadata deserialize(ByteBuffer content, long version, boolean useFirstFile) {
        final int formatVersion = content.getInt(FORMAT_VERSION_OFFSET);

        if (formatVersion != FORMAT_VERSION) {
            throw new ExodusException("Invalid format of startup metadata. { expected : " + FORMAT_VERSION +
                    ", actual: " + formatVersion + "}");
        }

        final int environmentFormatVersion = content.getInt(ENVIRONMENT_FORMAT_VERSION_OFFSET);
        final long dbRootAddress = content.getLong(DB_ROOT_ADDRESS_OFFSET);
        final int pageSize = content.getInt(PAGE_SIZE_OFFSET);
        final long fileLengthBoundary = content.getLong(FILE_LENGTH_BOUNDARY_OFFSET);
        final boolean closedFlag = content.get(CORRECTLY_CLOSED_FLAG_OFFSET) > 0;

        return new StartupMetadata(useFirstFile, dbRootAddress, closedFlag, pageSize, version,
                environmentFormatVersion,
                fileLengthBoundary);
    }

    public static ByteBuffer serialize(final long version, final int environmentFormatVersion,
                                       final long rootAddress, final int pageSize,
                                       final long fileLengthBoundary,
                                       final boolean correctlyClosedFlag) {
        final ByteBuffer content = ByteBuffer.allocate(FILE_SIZE);

        content.putLong(FILE_VERSION_OFFSET, version);
        content.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
        content.putInt(ENVIRONMENT_FORMAT_VERSION_OFFSET, environmentFormatVersion);
        content.putLong(DB_ROOT_ADDRESS_OFFSET, rootAddress);
        content.putInt(PAGE_SIZE_OFFSET, pageSize);
        content.putLong(FILE_LENGTH_BOUNDARY_OFFSET, fileLengthBoundary);
        content.put(CORRECTLY_CLOSED_FLAG_OFFSET, correctlyClosedFlag ? (byte) 1 : 0);

        final long hash = BufferedDataWriter.xxHash.hash(content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED);
        content.putLong(HASHCODE_OFFSET, hash);

        return content;
    }

    public static boolean isStartupFileName(String name) {
        return FIRST_FILE_NAME.equals(name) || SECOND_FILE_NAME.equals(name);
    }
}
