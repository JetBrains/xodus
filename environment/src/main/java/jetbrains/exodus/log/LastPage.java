package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

public class LastPage {
    @NotNull
    final byte[] bytes;
    public final long pageAddress;
    public final int count;

    public final long highAddress;
    public final long approvedHighAddress;

    @NotNull
    public final LogFileSetImmutable logFileSet;

    // empty
    LastPage(final long fileSize) {
        this(fileSize, 0, 0);
    }

    // fake page for closed log "residual state" info
    LastPage(final long fileSize, final long pageAddress, final long highAddress) {
        this.bytes = new byte[0];
        this.pageAddress = pageAddress;
        this.count = -1;
        this.highAddress = this.approvedHighAddress = highAddress;
        this.logFileSet = new LogFileSetImmutable(fileSize); // no files
    }

    // non-empty
    LastPage(@NotNull byte[] bytes, long pageAddress, int count, long highAddress, long approvedHighAddress, @NotNull final LogFileSetImmutable logFileSet) {
        this.bytes = bytes;
        this.pageAddress = pageAddress;
        this.count = count;
        this.highAddress = highAddress;
        this.approvedHighAddress = approvedHighAddress;
        this.logFileSet = logFileSet;
    }

    LastPage withApprovedAddress(long updatedApprovedHighAddress) {
        return new LastPage(bytes, pageAddress, count, highAddress, updatedApprovedHighAddress, logFileSet);
    }

    LastPage withResize(int updatedCount, long updatedHighAddress, long updatedApprovedHighAddress, @NotNull final LogFileSetImmutable logFileSet) {
        return new LastPage(bytes, pageAddress, updatedCount, updatedHighAddress, updatedApprovedHighAddress, logFileSet);
    }
}
