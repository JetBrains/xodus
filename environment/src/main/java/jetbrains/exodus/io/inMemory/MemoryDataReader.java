/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MemoryDataReader implements DataReader {

    private static final Logger logger = LoggerFactory.getLogger(MemoryDataReader.class);

    @NotNull
    private final Memory data;

    public MemoryDataReader(@NotNull Memory data) {
        this.data = data;
    }

    @Override
    public Block[] getBlocks() {
        final Collection<Memory.Block> blocks = data.getAllBlocks();
        final Block[] result = new Block[blocks.size()];
        int i = 0;
        for (final Memory.Block block : blocks) {
            result[i++] = new MemoryBlock(block);
        }
        FileDataReader.sortBlocks(result);
        return result;
    }

    @Override
    public void removeBlock(final long blockAddress, @NotNull final RemoveBlockType rbt) {
        if (!data.removeBlock(blockAddress)) {
            throw new ExodusException("There is no memory block by address " + blockAddress);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Deleted file " + LogUtil.getLogFilename(blockAddress));
        }
    }

    @Override
    public void truncateBlock(long blockAddress, long length) {
        data.getOrCreateBlockData(blockAddress, length);
        if (logger.isInfoEnabled()) {
            logger.info("Truncated file " + LogUtil.getLogFilename(blockAddress));
        }
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public String getLocation() {
        return data.toString();
    }

    @Override
    public Block getBlock(long address) {
        return new MemoryBlock(data.getBlockData(address));
    }

    private static final class MemoryBlock implements Block {
        @NotNull
        private final Memory.Block data;

        private MemoryBlock(@NotNull Memory.Block data) {
            this.data = data;
        }

        @Override
        public long getAddress() {
            return data.getAddress();
        }

        @Override
        public long length() {
            return data.getSize();
        }

        @Override
        public int read(final byte[] output, long position, int count) {
            return data.read(output, position, count);
        }

        @Override
        public boolean setReadOnly() {
            return false;
        }
    }
}