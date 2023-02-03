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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.io.Block;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LogTip {
    private static final byte[] NO_BYTES = new byte[0];

    @NotNull
    final byte[] bytes;
    public final long pageAddress;
    public final int count;

    public final long highAddress;
    public final long approvedHighAddress;

    @NotNull
    final BlockSet.Immutable blockSet;

    @Nullable
    private Iterable<Block> cachedBlocks;

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
    }

    // non-empty
    public LogTip(@NotNull byte[] bytes,
                  long pageAddress,
                  int count,
                  long highAddress,
                  long approvedHighAddress,
                  @NotNull final BlockSet.Immutable blockSet) {
        this.bytes = bytes;
        this.pageAddress = pageAddress;
        this.count = count;
        this.highAddress = highAddress;
        this.approvedHighAddress = approvedHighAddress;
        this.blockSet = blockSet;
    }

    public LogTip asTruncatedTo(final long highAddress) {
        return new LogTip(bytes, pageAddress, count, highAddress, highAddress, blockSet);
    }

    LogTip withApprovedAddress(long updatedApprovedHighAddress) {
        return new LogTip(bytes, pageAddress, count, highAddress, updatedApprovedHighAddress, blockSet);
    }

    LogTip withResize(int updatedCount, long updatedHighAddress, long updatedApprovedHighAddress, @NotNull final BlockSet.Immutable blockSet) {
        return new LogTip(bytes, pageAddress, updatedCount, updatedHighAddress, updatedApprovedHighAddress, blockSet);
    }

    public long[] getAllFiles() {
        return blockSet.getFiles();
    }

    public BlockSet.Mutable getBlockSetCopy() {
        return blockSet.beginWrite();
    }

    public LongIterator getFilesFrom(final long highAddress) {
        return blockSet.getFilesFrom(highAddress);
    }

    @Nullable
    public Iterable<Block> getCachedBlocks() {
        return cachedBlocks;
    }

    public void setCachedBlocks(@NotNull final Iterable<Block> cachedBlocks) {
        this.cachedBlocks = cachedBlocks;
    }
}
