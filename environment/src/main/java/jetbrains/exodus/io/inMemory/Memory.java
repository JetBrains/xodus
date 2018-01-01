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
package jetbrains.exodus.io.inMemory;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.LongObjectCache;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Map;

public class Memory {
    @Nullable
    private Block lastBlock;
    @NotNull
    private final LongHashMap<Block> data = new LongHashMap<>();
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @NotNull
    private final LongObjectCache<Block> removedBlocks = new LongObjectCache<>(100);

    protected Collection<Block> getAllBlocks() {
        return data.values();
    }

    protected Block getBlockData(long address) {
        synchronized (data) {
            return data.get(address);
        }
    }

    protected Block getOrCreateBlockData(long address, long length) {
        Block block;
        synchronized (data) {
            block = data.get(address);
            if (block == null) {
                final int initialSize = lastBlock == null ?
                        2048 :
                        lastBlock.getSize();
                lastBlock = block = new Block(address, initialSize);
                data.put(address, lastBlock);
            } else {
                if (block.getSize() != length) {
                    block.setSize(length);
                }
                lastBlock = block;
            }
        }
        return block;
    }

    protected boolean removeBlock(final long blockAddress) {
        final Block removed;
        synchronized (data) {
            removed = data.remove(blockAddress);
        }
        final boolean result = removed != null;
        if (result) {
            synchronized (data) {
                removedBlocks.cacheObject(blockAddress, removed);
            }
        }
        return result;
    }

    protected void clear() {
        synchronized (data) {
            data.clear();
        }
    }

    public void dump(@NotNull final File location) {
        location.mkdirs();
        final ObjectProcedure<Map.Entry<Long, Block>> saver = new ObjectProcedure<Map.Entry<Long, Block>>() {
            @Override
            public boolean execute(Map.Entry<Long, Block> object) {
                try {
                    final File dest = new File(location, LogUtil.getLogFilename(object.getKey()));
                    final RandomAccessFile output = new RandomAccessFile(dest, "rw");
                    final Block block = object.getValue();
                    output.write(block.getData(), 0, block.getSize());
                    output.close();
                    // output.getChannel().force(false);
                } catch (IOException e) {
                    throw new ExodusException(e);
                }
                return true;
            }
        };
        synchronized (data) {
            data.forEachEntry(saver);
            removedBlocks.forEachEntry(saver);
        }
    }

    static final class Block {

        private final long address;
        private int size;
        private byte[] data;

        Block(long address, int initialSize) {
            this.address = address;
            data = new byte[initialSize];
        }

        public long getAddress() {
            return address;
        }

        public int getSize() {
            return size;
        }

        public byte[] getData() {
            return data;
        }

        public void setSize(long size) {
            this.size = (int) size;
        }

        public void write(byte[] b, int off, int len) {
            final int newSize = size + len;
            ensureCapacity(newSize);
            System.arraycopy(b, off, data, size, len);
            size = newSize;
        }

        public int read(final byte[] output, long position, int count) {
            final long maxRead;
            if (position < 0 || (maxRead = size - position) < 0) {
                throw new ExodusException("Block index out of range");
            }
            if (maxRead < count) {
                count = (int) maxRead;
            }
            System.arraycopy(data, (int) position, output, 0, count);
            return count;
        }

        public void ensureCapacity(int minCapacity) {
            int oldCapacity = data.length;
            if (minCapacity > oldCapacity) {
                byte[] oldData = data;
                int newCapacity = oldCapacity * 3 / 2 + 1;
                if (newCapacity < minCapacity) newCapacity = minCapacity;
                data = new byte[newCapacity];
                System.arraycopy(oldData, 0, data, 0, oldCapacity);
            }
        }
    }
}
