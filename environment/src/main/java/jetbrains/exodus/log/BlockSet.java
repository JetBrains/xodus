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
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap;
import jetbrains.exodus.io.Block;

import java.util.Iterator;

public abstract class BlockSet {
    protected final long blockSize;
    protected final PersistentLongMap<Block> set;

    protected BlockSet(long blockSize, PersistentLongMap<Block> set) {
        this.blockSize = blockSize;
        this.set = set;
    }


    abstract protected PersistentLongMap.ImmutableMap<Block> getCurrent();

    public int size() {
        return getCurrent().size();
    }

    public boolean isEmpty() {
        return getCurrent().isEmpty();
    }

    public Long getMinimum() {
        var iterator = getCurrent().iterator();
        if (iterator.hasNext())
            return iterator.next().getKey() * blockSize;
        return null;
    }

    public Long getMaximum() {
        var iterator = getCurrent().reverseIterator();
        if (iterator.hasNext()) {
            return iterator.next().getKey() * blockSize;
        }
        return null;
    }

    public long[] array() {
        return getFiles(true);
    }

    long[] getFiles() {
        return getFiles(false);
    }

    long[] getFiles(boolean reversed) {
        var current = getCurrent();
        var result = new long[current.size()];
        Iterator<PersistentLongMap.Entry<Block>> iterator;
        if (reversed) {
            iterator = current.reverseIterator();
        } else {
            iterator = current.iterator();
        }
        for (var i = 0; i < result.length; i++) {
            result[i] = iterator.next().getKey() * blockSize;
        }

        return result;
    }

    public boolean contains(long blockAddress) {
        return getCurrent().containsKey(blockAddress / blockSize);
    }

    public Block getBlock(long blockAddress) {
        var result = getCurrent().get(blockAddress / blockSize);
        if (result != null) {
            return result;
        }
        return new EmptyBlock(blockAddress);
    }

    // if address is inside of a block, the block containing it must be included as well if present
    LongIterator getFilesFrom(long blockAddress) {
        return new LongIterator() {
            private final Iterator<PersistentLongMap.Entry<Block>> it;

            {

                if (blockAddress == 0L) {
                    it = getCurrent().iterator();
                } else {
                    it = getCurrent().tailEntryIterator(blockAddress / blockSize);
                }
            }

            @Override
            public long nextLong() {
                return it.next().getKey() * blockSize;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Long next() {
                return nextLong();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Mutable beginWrite() {
        return new Mutable(blockSize, set.getClone());
    }


    public static final class Immutable extends BlockSet {
        private final PersistentLongMap.ImmutableMap<Block> immutable;

        public Immutable(long blockSize) {
            this(blockSize, new PersistentBitTreeLongMap<>());
        }

        public Immutable(long blockSize, PersistentLongMap<Block> map) {
            super(blockSize, map);
            this.immutable = map.beginWrite();
        }

        @Override
        protected PersistentLongMap.ImmutableMap<Block> getCurrent() {
            return immutable;
        }
    }

    public static final class Mutable extends BlockSet {
        private final PersistentLongMap.MutableMap<Block> mutable;


        public Mutable(long blockSize, PersistentLongMap<Block> map) {
            super(blockSize, map);
            this.mutable = map.beginWrite();
        }


        @Override
        protected PersistentLongMap.ImmutableMap<Block> getCurrent() {
            return mutable;
        }

        public void clear() {
            mutable.clear();
        }

        public void add(long blockAddress, Block block) {
            mutable.put(blockAddress / blockSize, block);
        }

        public boolean remove(long blockAddress) {
            return mutable.remove(blockAddress / blockSize) != null;
        }

        public Immutable endWrite() {
            if (!mutable.endWrite()) {
                throw new IllegalStateException("File set can't be updated");
            }
            return new Immutable(blockSize, set.getClone());
        }
    }

    @SuppressWarnings("ClassCanBeRecord")
    private static final class EmptyBlock implements Block {
        private final long address;

        private EmptyBlock(long address) {
            this.address = address;
        }

        @Override
        public long getAddress() {
            return address;
        }

        @Override
        public long length() {
            return 0L;
        }

        @Override
        public int read(byte[] output, long position, int offset, int count) {
            return 0;
        }

        @Override
        public Block refresh() {
            return this;
        }
    }
}
