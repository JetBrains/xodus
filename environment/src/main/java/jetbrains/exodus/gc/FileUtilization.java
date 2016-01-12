/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.gc;

import jetbrains.exodus.core.dataStructures.skiplists.LongIntSkipList;
import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;

final class FileUtilization {

    private long freeBytes;
    private SoftReference<LongIntSkipList> freeSpace;

    FileUtilization(long freeBytes) {
        this.freeBytes = freeBytes;
        freeSpace = new SoftReference<>(new LongIntSkipList());
    }

    FileUtilization() {
        this(0);
    }

    long getFreeBytes() {
        return freeBytes;
    }

    boolean isExpired(@NotNull final Loggable loggable) {
        return isExpired(loggable.getAddress(), 1);
    }

    boolean isExpired(final long startAddress, final int length) {
        final LongIntSkipList.SkipListNode node = getFreeSpace().getLessOrEqual(startAddress);
        return node != null && node.getKey() + node.getValue() >= startAddress + length;
    }

    void fetchExpiredLoggable(@NotNull final Loggable loggable, @NotNull final LongIntSkipList freeSpace) {
        final long address = loggable.getAddress();
        final int length = loggable.length();
        freeBytes += length;
        final LongIntSkipList.SkipListNode node = freeSpace.getLessOrEqual(address);
        if (node == null) {
            freeSpace.add(address, length);
        } else {
            final long freeSpaceStart = node.getKey();
            final int freeSpaceLength = node.getValue();
            if (freeSpaceStart + freeSpaceLength >= address) {
                node.setValue(Math.max(freeSpaceLength, (int) (address - freeSpaceStart + length)));
            } else {
                freeSpace.add(address, length);
            }
        }
        final LongIntSkipList.SkipListNode floorNode = freeSpace.getLessOrEqual(address);
        if (floorNode != null) {
            final LongIntSkipList.SkipListNode ceilingNode = freeSpace.getNext(floorNode);
            if (ceilingNode != null) {
                final long leftStart = floorNode.getKey();
                final int leftLength = floorNode.getValue();
                final long nextStart = ceilingNode.getKey();
                if (leftStart + leftLength == nextStart) {
                    floorNode.setValue(leftLength + ceilingNode.getValue());
                    freeSpace.remove(nextStart);
                }
            }
        }
    }

    @NotNull
    LongIntSkipList getFreeSpace() {
        LongIntSkipList result = freeSpace.get();
        if (result == null) {
            result = new LongIntSkipList();
            freeSpace = new SoftReference<>(result);
        }
        return result;
    }
}
