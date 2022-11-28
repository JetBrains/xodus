package jetbrains.exodus.log;

import jetbrains.exodus.InvalidSettingException;
import jetbrains.exodus.bindings.BindingUtils;
import jetbrains.exodus.crypto.EnvKryptKt;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataWriter;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public abstract class BufferedDataWriter {
    public static final long XX_HASH_SEED = 0xADEF1279AL;
    public static final XXHashFactory XX_HASH_FACTORY = XXHashFactory.fastestJavaInstance();
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();

    public static final int HASH_CODE_SIZE = Long.BYTES;
    public static final int FIRST_ITERABLE_OFFSET_SIZE = Integer.BYTES;
    public static final int FIRST_ITERABLE_OFFSET = HASH_CODE_SIZE + FIRST_ITERABLE_OFFSET_SIZE;

    public static final int LOGGABLE_DATA = FIRST_ITERABLE_OFFSET;

    @NotNull
    protected final Log log;
    @NotNull
    protected final LogCache logCache;

    protected final DataWriter writer;

    @NotNull
    protected final LogTip initialPage;
    @Nullable
    protected final StreamCipherProvider cipherProvider;
    protected final byte[] cipherKey;
    protected final long cipherBasicIV;
    protected final int pageSize;

    protected final int pageSizeMask;

    protected final int adjustedPageSize;
    @NotNull
    protected final BlockSet.Mutable blockSetMutable;

    @NotNull
    protected SyncBufferedDataWriter.MutablePage currentPage;
    protected long highAddress;


    protected final boolean calculateHashCode;


    BufferedDataWriter(@NotNull final Log log,
                       @NotNull DataWriter writer,
                       @NotNull final LogTip page, boolean calculateHashCode) {
        this.log = log;
        this.writer = writer;

        logCache = log.cache;
        this.blockSetMutable = page.blockSet.beginWrite();
        this.initialPage = page;
        this.highAddress = page.highAddress;
        this.calculateHashCode = calculateHashCode;
        final boolean validInitialPage = page.count >= 0;
        pageSize = log.getCachePageSize();

        if (validInitialPage) {
            if (pageSize != page.bytes.length) {
                throw new InvalidSettingException("Configured page size doesn't match actual page size, pageSize = " +
                        pageSize + ", actual page size = " + page.bytes.length);
            }

            currentPage = new SyncBufferedDataWriter.MutablePage(null, page.bytes, page.pageAddress, page.count);
            currentPage.xxHash64 = page.xxHash64;
            currentPage.firstLoggable = BindingUtils.readInt(page.bytes, pageSize - LOGGABLE_DATA);
        } else {
            byte[] newPage = logCache.allocPage();
            BindingUtils.writeInt(-1, newPage, pageSize - LOGGABLE_DATA);
            currentPage = new SyncBufferedDataWriter.MutablePage(null, newPage, page.pageAddress, 0);
            currentPage.xxHash64 = XX_HASH_FACTORY.newStreamingHash64(XX_HASH_SEED);
        }

        cipherProvider = log.getConfig().getCipherProvider();
        cipherKey = log.getConfig().getCipherKey();
        cipherBasicIV = log.getConfig().getCipherBasicIV();

        pageSizeMask = (pageSize - 1);
        adjustedPageSize = pageSize - LOGGABLE_DATA;

        assert blockSetMutable.getMaximum() == null ||
                blockSetMutable.getBlock(blockSetMutable.getMaximum()).length() % log.getFileLengthBound() ==
                        highAddress % log.getFileLengthBound();
    }

    public static void checkPageConsistency(long pageAddress, byte @NotNull [] bytes, int pageSize, Log log) {
        if (pageSize != bytes.length) {
            DataCorruptionException.raise("Unexpected page size (bytes). {expected " + pageSize
                    + ": , actual : " + bytes.length + "}", log, pageAddress);
        }

        final XXHash64 xxHash = SyncBufferedDataWriter.xxHash;
        final long calculatedHash = xxHash.hash(bytes, 0,
                bytes.length - HASH_CODE_SIZE, SyncBufferedDataWriter.XX_HASH_SEED);
        final long storedHash = BindingUtils.readLong(bytes, pageSize - HASH_CODE_SIZE);

        if (storedHash != calculatedHash) {
            DataCorruptionException.raise("Page is broken. Expected and calculated hash codes are different.",
                    log, pageAddress);
        }
    }

    public static void updateHashCode(final byte @NotNull [] bytes) {
        final int hashCodeOffset = bytes.length - SyncBufferedDataWriter.HASH_CODE_SIZE;
        final long hash =
                SyncBufferedDataWriter.xxHash.hash(bytes, 0, hashCodeOffset,
                        SyncBufferedDataWriter.XX_HASH_SEED);

        BindingUtils.writeLong(hash, bytes, hashCodeOffset);
    }

    public static void writeFirstLoggableOffset(final byte @NotNull [] bytes, int offset) {
        BindingUtils.writeInt(offset, bytes, bytes.length - SyncBufferedDataWriter.LOGGABLE_DATA);
    }

    @NotNull
    public BlockSet.Mutable getBlockSetMutable() {
        return blockSetMutable;
    }

    public void setHighAddress(long highAddress) {
        allocLastPage(highAddress - (((int) highAddress) & (log.getCachePageSize() - 1))); // don't alloc full page
        this.highAddress = highAddress;
    }

    public void allocLastPage(long pageAddress) {
        SyncBufferedDataWriter.MutablePage result = currentPage;

        if (pageAddress == result.pageAddress) {
            return;
        }

        result = new SyncBufferedDataWriter.MutablePage(null, logCache.allocPage(), pageAddress, 0);
        currentPage = result;
    }

    abstract void write(byte b, long firstLoggable);

    abstract void write(byte[] b, int offset, int len);

    abstract void commit();

    abstract void flush();

    abstract boolean padWithNulls(long fileLengthBound, byte[] nullPage);

    abstract public void padWholePageWithNulls();

    abstract public int padPageWithNulls();

    Block openOrCreateBlock(long address, long length) {
        return writer.openOrCreateBlock(address, length);
    }

    long getHighAddress() {
        return highAddress;
    }

    boolean fitsIntoSingleFile(long fileLengthBound, int loggableSize) {
        final long fileAddress = highAddress / fileLengthBound;
        final long nextFileAddress =
                (log.adjustLoggableAddress(highAddress, loggableSize) - 1) / fileLengthBound;

        return fileAddress == nextFileAddress;
    }

    boolean isFileFull(long fileLengthBound) {
        return highAddress % fileLengthBound == 0;
    }

    @NotNull
    LogTip getStartingTip() {
        return initialPage;
    }


    @NotNull
    LogTip getUpdatedTip() {
        final SyncBufferedDataWriter.MutablePage currentPage = this.currentPage;
        final BlockSet.Immutable blockSetImmutable = blockSetMutable.endWrite();

        return new LogTip(currentPage.bytes, currentPage.pageAddress,
                currentPage.committedCount, highAddress, highAddress,
                currentPage.xxHash64, blockSetImmutable);
    }

    byte getByte(final long address, final byte max) {
        final int offset = ((int) address) & (pageSize - 1);
        final long pageAddress = address - offset;
        final SyncBufferedDataWriter.MutablePage page = getWrittenPage(pageAddress);
        if (page != null) {
            final byte result = (byte) (page.bytes[offset] ^ 0x80);
            if (result < 0 || result > max) {
                throw new IllegalStateException("Unknown written page loggable type: " + result);
            }
            return result;
        }

        // slow path: unconfirmed file saved to disk, read byte from it
        final long fileAddress = log.getFileAddress(address);
        if (!blockSetMutable.contains(fileAddress)) {
            BlockNotFoundException.raise("Address is out of log space, underflow", log, address);
        }

        final byte[] output = new byte[pageSize];

        final Block block = blockSetMutable.getBlock(fileAddress);

        final int readBytes = block.read(output, pageAddress - fileAddress, 0, output.length);
        if (readBytes < pageSize) {
            DataCorruptionException.raise("Unexpected page size (bytes). {expected " + pageSize
                    + ": , actual : " + readBytes + "}", log, pageAddress);
        }

        checkPageConsistency(pageAddress, output, pageSize, log);

        if (cipherProvider != null) {
            EnvKryptKt.cryptBlocksMutable(cipherProvider, cipherKey, cipherBasicIV, pageAddress, output, 0,
                    pageSize - HASH_CODE_SIZE, LogUtil.LOG_BLOCK_ALIGNMENT);
        }

        final byte result = (byte) (output[offset] ^ 0x80);
        if (result < 0 || result > max) {
            throw new IllegalStateException("Unknown written file loggable type: " + result + ", address: " + address);
        }

        return result;
    }

    // warning: this method is O(N), where N is number of added pages
    private SyncBufferedDataWriter.MutablePage getWrittenPage(long alignedAddress) {
        SyncBufferedDataWriter.MutablePage currentPage = this.currentPage;
        do {
            final long highPageAddress = currentPage.pageAddress;
            if (alignedAddress == highPageAddress) {
                return currentPage;
            }
            currentPage = currentPage.previousPage;
        } while (currentPage != null);
        return null;
    }


    protected void cachePage(final byte @NotNull [] bytes, final long pageAddress) {
        logCache.cachePage(log, pageAddress, bytes);
    }

    protected void writeFirstLoggableOffset(long firstLoggable, MutablePage currentPage) {
        if (firstLoggable >= 0 && currentPage.firstLoggable < 0) {
            int loggableOffset = (int) (firstLoggable & pageSizeMask);

            currentPage.firstLoggable = loggableOffset;
            writeFirstLoggableOffset(currentPage.bytes, loggableOffset);
        }
    }

    protected int doPadWholePageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;

        if (writtenInPage > 0) {
            final int written = pageSize - writtenInPage;

            Arrays.fill(currentPage.bytes, writtenInPage, pageSize, (byte) 0x80);

            currentPage.writtenCount = pageSize;
            highAddress += written;

            return written;
        }

        return 0;
    }

    protected int doPadPageWithNulls() {
        final int writtenInPage = currentPage.writtenCount;
        if (writtenInPage > 0) {
            final int pageDelta = adjustedPageSize - writtenInPage;

            int written = 0;
            if (pageDelta > 0) {
                Arrays.fill(currentPage.bytes, writtenInPage, adjustedPageSize, (byte) 0x80);
                currentPage.writtenCount = pageSize;

                written = pageDelta + LOGGABLE_DATA;
            }

            return written;
        } else {
            return 0;
        }
    }


    public static byte[] generateNullPage(int pageSize) {
        final byte[] data = new byte[pageSize];
        Arrays.fill(data, 0, pageSize - LOGGABLE_DATA, (byte) 0x80);

        final long hash = xxHash.hash(data, 0, pageSize - HASH_CODE_SIZE, XX_HASH_SEED);
        BindingUtils.writeLong(hash, data, pageSize - HASH_CODE_SIZE);

        return data;
    }


    public static class MutablePage {

        @Nullable
        MutablePage previousPage;
        byte @NotNull [] bytes;
        final long pageAddress;
        int flushedCount;
        int committedCount;
        int writtenCount;
        int firstLoggable;

        StreamingXXHash64 xxHash64;

        MutablePage(@Nullable final MutablePage previousPage, final byte @NotNull [] page,
                    final long pageAddress, final int count) {
            this.previousPage = previousPage;
            this.bytes = page;
            this.pageAddress = pageAddress;
            flushedCount = committedCount = writtenCount = count;
            this.firstLoggable = -1;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public int getCount() {
            return writtenCount;
        }
    }
}
