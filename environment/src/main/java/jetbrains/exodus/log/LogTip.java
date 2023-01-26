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

import org.jetbrains.annotations.NotNull;

public class LogTip {
    private static final byte[] NO_BYTES = new byte[0];

    final byte @NotNull [] bytes;

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
    }

    public LogTip(byte @NotNull [] bytes,
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

    public long[] getAllFiles() {
        return blockSet.getFiles();
    }

    public BlockSet.Mutable getBlockSetCopy() {
        return blockSet.beginWrite();
    }
}
