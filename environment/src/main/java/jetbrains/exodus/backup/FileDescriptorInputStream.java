package jetbrains.exodus.backup;

import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.log.BufferedDataWriter;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class FileDescriptorInputStream extends InputStream {
    private final @NotNull FileInputStream fileInputStream;

    private final long fileAddress;
    private final int pageSize;

    private final long backupFileSize;
    private final long storedDataFilesSize;

    private final byte @NotNull [] page;
    private int pagePosition;

    private int position;

    @Nullable
    private final StreamCipherProvider cipherProvider;

    private final byte @Nullable [] cipherKey;
    private final long cipherBasicIV;


    public FileDescriptorInputStream(final @NotNull FileInputStream fileInputStream, long fileAddress, int pageSize,
                                     long backupFileSize, long storedDataFilesSize,
                                     @Nullable StreamCipherProvider cipherProvider,
                                     byte @Nullable [] cipherKey, long cipherBasicIV) {
        this.fileInputStream = fileInputStream;
        this.fileAddress = fileAddress;
        this.pageSize = pageSize;
        page = new byte[pageSize];

        //backup always contains only full pages
        this.backupFileSize = backupFileSize;
        this.storedDataFilesSize = storedDataFilesSize;
        this.cipherProvider = cipherProvider;
        this.cipherKey = cipherKey;
        this.cipherBasicIV = cipherBasicIV;

        this.pagePosition = Integer.MAX_VALUE;
    }

    @Override
    public int read() throws IOException {
        if (position >= backupFileSize) {
            return -1;
        }

        if (pagePosition < pageSize) {
            final int datum = page[pagePosition];

            pagePosition++;
            position++;

            return datum;
        }

        if (readPage()) {
            final int datum = page[0];

            pagePosition = 1;
            position++;

            return datum;
        }

        return -1;
    }

    public int read(byte @NotNull [] out, int off, int len) throws IOException {
        if (off < 0 || len < 0 || out.length < off + len) {
            throw new IndexOutOfBoundsException();
        }

        if (len == 0) {
            return 0;
        }

        if (position >= backupFileSize) {
            return -1;
        }

        if (pagePosition >= pageSize) {
            if (!readPage()) {
                return -1;
            }
        }

        final int resultSize = Math.min(pageSize - pagePosition, len);
        System.arraycopy(page, pagePosition, out, off, resultSize);

        pagePosition += resultSize;
        position += resultSize;


        return resultSize;
    }


    @Override
    public void close() throws IOException {
        fileInputStream.close();
        super.close();
    }

    @Override
    public int available() throws IOException {
        final long available = backupFileSize - position;

        if (available <= Integer.MAX_VALUE) {
            return (int) available;
        }

        return 0;
    }

    private boolean readPage() throws IOException {
        final int toRead = (int) Math.min(pageSize, storedDataFilesSize - position);
        assert toRead >= 0;

        int read = 0;
        while (read < toRead) {
            final int r = fileInputStream.read(page, read, toRead - read);

            if (r == -1) {
                if (read == 0) {
                    return false;
                } else {
                    break;
                }
            }

            read += r;
        }

        if (read < pageSize) {
            Arrays.fill(page, read, pageSize - BufferedDataWriter.LOGGABLE_DATA, (byte) 0x80);

            if (cipherProvider != null) {
                assert cipherKey != null;

                final long pageAddress = fileAddress + position;
                EnvKryptKt.cryptBlocksMutable(cipherProvider, cipherKey,
                        cipherBasicIV, pageAddress, page, read, pageSize - BufferedDataWriter.LOGGABLE_DATA - read,
                        LogUtil.LOG_BLOCK_ALIGNMENT);
            }

            BufferedDataWriter.updateHashCode(page);
        }

        pagePosition = 0;
        return true;
    }
}
