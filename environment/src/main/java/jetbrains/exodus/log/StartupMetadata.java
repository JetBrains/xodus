package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.util.IOUtil;

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

    private static final int DB_ROOT_ADDRESS_OFFSET = FORMAT_VERSION_OFFSET + FORMAT_VERSION_BYTES;
    private static final int DB_ROOT_BYTES = Long.BYTES;

    private static final int PAGE_SIZE_OFFSET = DB_ROOT_ADDRESS_OFFSET + DB_ROOT_BYTES;
    private static final int PAGE_SIZE_BYTES = Integer.BYTES;

    private static final int CORRECTLY_CLOSED_FLAG_OFFSET = PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES;
    public static final int CLOSED_FLAG_BYTES = Byte.BYTES;

    private static final int FILE_SIZE = CORRECTLY_CLOSED_FLAG_OFFSET + CLOSED_FLAG_BYTES;

    static final String FIRST_FILE_NAME = "startup-metadata-0";
    static final String SECOND_FILE_NAME = "startup-metadata-1";


    static final int FORMAT_VERSION = 1;

    private final boolean useFirstFile;

    private volatile long rootAddress;
    private final boolean isCorrectlyClosed;
    private final int pageSize;
    private final long currentVersion;

    private StartupMetadata(final boolean useFirstFile, final long rootAddress,
                            final boolean isCorrectlyClosed, int pageSize, long currentVersion) {
        this.useFirstFile = useFirstFile;
        this.rootAddress = rootAddress;
        this.isCorrectlyClosed = isCorrectlyClosed;
        this.pageSize = pageSize;
        this.currentVersion = currentVersion;
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
        final ByteBuffer content = serialize(currentVersion, rootAddress, pageSize, true);
        store(content, dbPath, useFirstFile);
    }

    public static StartupMetadata open(final FileDataReader reader, final boolean isReadOnly,
                                       final int pageSize) throws IOException {
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
            final boolean correctlyClosed = !reader.getBlocks().iterator().hasNext();
            return new StartupMetadata(true, -1, correctlyClosed,
                    pageSize, 0);
        }

        final StartupMetadata result = deserialize(content, nextVersion + 1, !useFirstFile);

        if (!isReadOnly) {
            final ByteBuffer updatedMetadata = serialize(nextVersion, -1,
                    result.getPageSize(), false);
            store(updatedMetadata, dbPath, useFirstFile);
        }

        return result;
    }

    public static StartupMetadata createStub(int pageSize) {
        return new StartupMetadata(true, -1, true, pageSize, 1);
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

        final long dbRootAddress = content.getLong(DB_ROOT_ADDRESS_OFFSET);
        final int pageSize = content.getInt(PAGE_SIZE_OFFSET);
        final boolean closedFlag = content.get(CORRECTLY_CLOSED_FLAG_OFFSET) > 0;

        return new StartupMetadata(useFirstFile, dbRootAddress, closedFlag, pageSize, version);
    }

    private static ByteBuffer serialize(final long version, final long rootAddress, final int pageSize,
                                        final boolean correctlyClosedFlag) {
        final ByteBuffer content = ByteBuffer.allocate(FILE_SIZE);

        content.putLong(FILE_VERSION_OFFSET, version);
        content.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
        content.putLong(DB_ROOT_ADDRESS_OFFSET, rootAddress);
        content.putInt(PAGE_SIZE_OFFSET, pageSize);
        content.put(CORRECTLY_CLOSED_FLAG_OFFSET, correctlyClosedFlag ? (byte) 1 : 0);

        final long hash = BufferedDataWriter.xxHash.hash(content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED);
        content.putLong(HASHCODE_OFFSET, hash);

        return content;
    }
}
