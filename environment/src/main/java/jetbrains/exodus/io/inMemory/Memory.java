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
package jetbrains.exodus.io.inMemory;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.LongObjectCache;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;

import java.util.Collection;

public final class Memory {
    private Block lastBlock;
    private final LongHashMap<Block> data = new LongHashMap<>();
    private final LongObjectCache<Block> removedBlocks = new LongObjectCache<>(100);

    public Collection<Memory.Block> getAllBlocks() {
        return data.values();
    }

    Block getOrCreateBlockData(long address, long length) {
        synchronized (data) {
            var block = data.get(address);
            if (block != null) {
                if (block.size != length) {
                    block.setSize((int) length);
                }
                lastBlock = block;
            } else {
                block = new Block(address, lastBlock != null ? lastBlock.size : 2048);
                lastBlock = block;
                data.put(address, lastBlock);
            }
            return block;
        }
    }

    public boolean removeBlock(long blockAddress) {
        synchronized (data) {
            var removed = data.remove(blockAddress);
            if (removed != null) {
                removedBlocks.cacheObject(blockAddress, removed);
                return true;
            }
            return false;
        }
    }

    public void clear() {
        synchronized (data) {
            data.clear();
        }
    }

    public static final class Block implements jetbrains.exodus.io.Block {
        private byte[] data;
        private int size = 0;

        private final long _address;

        public Block(final long address, final int initialSize) {
            _address = address;
            data = new byte[initialSize];
        }

        public int getSize() {
            return size;
        }

        @Override
        public long getAddress() {
            return _address;
        }

        public long length() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public void write(byte[] b, int off, int len) {
            int newSize = size + len;
            ensureCapacity(newSize);
            System.arraycopy(b, off, data, size, len);
            size = newSize;
        }

        public int read(byte[] output, long position, int offset, int count) {
            var result = count;
            if (position < 0) {
                throw new ExodusException("Block index out of range, underflow");
            }
            var maxRead = size - position;
            if (maxRead < 0) {
                throw new ExodusException("Block index out of range");
            }
            if (maxRead < result) {
                result = (int) maxRead;
            }
            System.arraycopy(data, (int) position, output, offset, result);
            return result;
        }

        @Override
        public Block refresh() {
            return this;
        }

        void ensureCapacity(int minCapacity) {
            if (minCapacity > data.length) {
                var oldCapacity = data.length;
                var newCapacity = oldCapacity * 3 / 2 + 1;
                if (newCapacity < minCapacity) {
                    newCapacity = minCapacity;
                }
                var oldData = data;
                data = new byte[newCapacity];
                System.arraycopy(oldData, 0, data, 0, oldCapacity);
            }
        }

        public void truncate(long newSize) {
            if (newSize < 0) {
                throw new IllegalArgumentException("Invalid size of the block " + newSize);
            }
            size = (int) newSize;
        }
    }
}
