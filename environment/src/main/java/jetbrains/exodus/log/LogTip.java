/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import net.jpountz.xxhash.StreamingXXHash64;
import org.jetbrains.annotations.NotNull;

public class LogTip {
    private static final byte[] NO_BYTES = new byte[0];

    final byte @NotNull [] bytes;
    final StreamingXXHash64 xxHash64;

    public final long pageAddress;
    public final int count;

    public final long highAddress;
    public final long approvedHighAddress;

    @NotNull
    final BlockSet.Immutable blockSet;

    // empty
    LogTip(final long fileLengthBound) {
        this(fileLengthBound, 0, 0);
    }

    // fake page for closed log "residual state" info
    LogTip(final long fileLengthBound, final long pageAddress, final long highAddress) {
        this.bytes = NO_BYTES;
        this.pageAddress = pageAddress;
        this.count = -1;
        this.highAddress = this.approvedHighAddress = highAddress;
        this.blockSet = new BlockSet.Immutable(fileLengthBound); // no files
        this.xxHash64 = SyncBufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(SyncBufferedDataWriter.XX_HASH_SEED);
    }

    public LogTip(byte @NotNull [] bytes,
                  long pageAddress,
                  int count,
                  long highAddress,
                  long approvedHighAddress,
                  @NotNull final BlockSet.Immutable blockSet) {
        this(bytes, pageAddress, count, highAddress, approvedHighAddress,
                createHash(pageAddress, highAddress, bytes), blockSet);
    }

    // non-empty
    public LogTip(byte @NotNull [] bytes,
                  long pageAddress,
                  int count,
                  long highAddress,
                  long approvedHighAddress,
                  final StreamingXXHash64 xxHash64,
                  @NotNull final BlockSet.Immutable blockSet) {
        this.bytes = bytes;
        this.pageAddress = pageAddress;
        this.count = count;
        this.highAddress = highAddress;
        this.approvedHighAddress = approvedHighAddress;
        this.blockSet = blockSet;
        this.xxHash64 = xxHash64;
    }

    public LogTip asTruncatedTo(final long highAddress) {
        updatePageHash(highAddress);

        return new LogTip(bytes, pageAddress, count, highAddress, highAddress, xxHash64, blockSet);
    }

    LogTip withApprovedAddress(long updatedApprovedHighAddress) {
        return new LogTip(bytes, pageAddress, count, highAddress, updatedApprovedHighAddress, xxHash64, blockSet);
    }

    private static StreamingXXHash64 createHash(final long pageAddress, final long highAddress, final byte[] bytes) {
        final int len = (int) (highAddress - pageAddress);

        final StreamingXXHash64 xxHash64 =
                SyncBufferedDataWriter.XX_HASH_FACTORY.newStreamingHash64(SyncBufferedDataWriter.XX_HASH_SEED);
        xxHash64.update(bytes, 0, len);

        return xxHash64;
    }

    private void updatePageHash(long updatedHighAddress) {
        if (updatedHighAddress > highAddress) {
            final int len = (int) (updatedHighAddress - highAddress);
            final int offset = (int) (highAddress - pageAddress);

            xxHash64.update(bytes, offset, len);
        } else if (updatedHighAddress < highAddress) {
            final int len = (int) (updatedHighAddress - pageAddress);

            xxHash64.reset();
            xxHash64.update(bytes, 0, len);
        }
    }

    public long[] getAllFiles() {
        return blockSet.getFiles();
    }

    public BlockSet.Mutable getBlockSetCopy() {
        return blockSet.beginWrite();
    }
}
