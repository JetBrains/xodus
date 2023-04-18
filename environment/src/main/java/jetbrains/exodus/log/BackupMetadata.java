package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;

import java.nio.ByteBuffer;

@SuppressWarnings({"unused", "FieldCanBeLocal"})
public class BackupMetadata extends StartupMetadata {
    public static final String BACKUP_METADATA_FILE_NAME = "backup_metadata";

    static final int LAST_FILE_ADDRESS = StartupMetadata.FILE_SIZE;
    static final int LAST_FILE_OFFSET = LAST_FILE_ADDRESS + Long.BYTES;
    static final int FILE_SIZE = LAST_FILE_OFFSET + Long.BYTES;

    private final long lastFileAddress;
    private final long lastFileOffset;

    protected BackupMetadata(boolean useFirstFile, long rootAddress,
                             boolean isCorrectlyClosed, int pageSize, long currentVersion,
                             int environmentFormatVersion, long fileLengthBoundary,
                             long lastFileAddress, long fileOffset) {
        super(useFirstFile, rootAddress, isCorrectlyClosed, pageSize, currentVersion,
                environmentFormatVersion, fileLengthBoundary);
        this.lastFileAddress = lastFileAddress;
        this.lastFileOffset = fileOffset;
    }

    public static ByteBuffer serialize(final long version, final int environmentFormatVersion,
                                       final long rootAddress, final int pageSize,
                                       final long fileLengthBoundary,
                                       final boolean correctlyClosedFlag, final long lastFileAddress,
                                       final long lastFileOffset) {
        final ByteBuffer content = ByteBuffer.allocate(FILE_SIZE);

        content.putLong(FILE_VERSION_OFFSET, version);
        content.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
        content.putInt(ENVIRONMENT_FORMAT_VERSION_OFFSET, environmentFormatVersion);
        content.putLong(DB_ROOT_ADDRESS_OFFSET, rootAddress);
        content.putInt(PAGE_SIZE_OFFSET, pageSize);
        content.putLong(FILE_LENGTH_BOUNDARY_OFFSET, fileLengthBoundary);
        content.put(CORRECTLY_CLOSED_FLAG_OFFSET, correctlyClosedFlag ? (byte) 1 : 0);
        content.putLong(LAST_FILE_ADDRESS, lastFileAddress);
        content.putLong(LAST_FILE_OFFSET, lastFileOffset);

        final long hash = BufferedDataWriter.xxHash.hash(content, FILE_VERSION_OFFSET, FILE_SIZE - HASH_CODE_SIZE,
                BufferedDataWriter.XX_HASH_SEED);
        content.putLong(HASHCODE_OFFSET, hash);

        return content;
    }


    public static StartupMetadata deserialize(ByteBuffer content, long version, boolean useFirstFile) {
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
        final long lastFileAddress = content.getLong(LAST_FILE_ADDRESS);
        final long lastFileOffset = content.getLong(LAST_FILE_OFFSET);

        return new BackupMetadata(useFirstFile, dbRootAddress, closedFlag, pageSize, version,
                environmentFormatVersion, fileLengthBoundary, lastFileAddress, lastFileOffset);
    }
}
