package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

class LastPage {
    static final LastPage EMPTY = new LastPage();

    @NotNull
    final byte[] bytes;
    final long pageAddress;
    final int count;

    final long highAddress;
    final long approvedHighAddress;

    // empty
    private LastPage() {
        this.bytes = new byte[0];
        this.pageAddress = 0;
        this.count = -1;
        this.highAddress = this.approvedHighAddress = 0;
    }

    // non-empty
    LastPage(@NotNull byte[] bytes, long pageAddress, int count, long highAddress, long approvedHighAddress) {
        this.bytes = bytes;
        this.pageAddress = pageAddress;
        this.count = count;
        this.highAddress = highAddress;
        this.approvedHighAddress = approvedHighAddress;
    }

    LastPage withApprovedAddress(long updatedApprovedHighAddress) {
        return new LastPage(bytes, pageAddress, count, highAddress, updatedApprovedHighAddress);
    }

    LastPage withResize(int updatedCount, long updatedHighAddress, long updatedApprovedHighAddress) {
        return new LastPage(bytes, pageAddress, updatedCount, updatedHighAddress, updatedApprovedHighAddress);
    }
}
