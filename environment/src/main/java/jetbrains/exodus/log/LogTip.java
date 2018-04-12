/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import org.jetbrains.annotations.NotNull;

public class LogTip {
    private static final byte[] NO_BYTES = new byte[0];

    @NotNull
    final byte[] bytes;
    public final long pageAddress;
    public final int count;

    public final long highAddress;
    public final long approvedHighAddress;

    @NotNull
    final LogFileSet.Immutable logFileSet;

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
        this.logFileSet = new LogFileSet.Immutable(fileLengthBound); // no files
    }

    // non-empty
    LogTip(@NotNull byte[] bytes, long pageAddress, int count, long highAddress, long approvedHighAddress, @NotNull final LogFileSet.Immutable logFileSet) {
        this.bytes = bytes;
        this.pageAddress = pageAddress;
        this.count = count;
        this.highAddress = highAddress;
        this.approvedHighAddress = approvedHighAddress;
        this.logFileSet = logFileSet;
    }

    LogTip withApprovedAddress(long updatedApprovedHighAddress) {
        return new LogTip(bytes, pageAddress, count, highAddress, updatedApprovedHighAddress, logFileSet);
    }

    LogTip withResize(int updatedCount, long updatedHighAddress, long updatedApprovedHighAddress, @NotNull final LogFileSet.Immutable logFileSet) {
        return new LogTip(bytes, pageAddress, updatedCount, updatedHighAddress, updatedApprovedHighAddress, logFileSet);
    }

    public long[] getAllFiles() {
        return logFileSet.getFiles();
    }

    public LongIterator getFilesFrom(final long highAddress) {
        return logFileSet.getFilesFrom(highAddress);
    }
}
