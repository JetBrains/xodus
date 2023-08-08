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

import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.DataReader;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public final class MemoryDataReader implements DataReader {
    private final Memory memory;

    public MemoryDataReader(Memory memory) {
        this.memory = memory;
    }

    public Memory getMemory() {
        return memory;
    }

    @NotNull
    public Iterable<Block> getBlocks() {
        var blocks = memory.getAllBlocks();
        var blocksList = new ArrayList<>(blocks);
        blocksList.sort(Comparator.comparingLong(Memory.Block::getAddress));

        return Collections.unmodifiableList(blocksList);
    }

    @NotNull
    public Iterable<Block> getBlocks(long fromAddress) {
        var blocks = memory.getAllBlocks();
        var blocksList = new ArrayList<Memory.Block>(blocks.size());
        for (var block : blocks) {
            if (block.getAddress() >= fromAddress) {
                blocksList.add(block);
            }
        }

        return Collections.unmodifiableList(blocksList);
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    @NotNull
    public String getLocation() {
        return memory.toString();
    }

    public static final class MemoryBlock implements Block {
        private final Memory.Block data;

        private MemoryBlock(Memory.Block data) {
            this.data = data;
        }

        @Override
        public long getAddress() {
            return data.getAddress();
        }

        public long length() {
            return data.length();
        }

        public int read(byte[] output, long position, int offset, int count) {
            return data.read(output, position, offset, count);
        }


        public void write(byte[] b, int off, int len) {
            data.write(b, off, len);
        }

        public MemoryBlock refresh() {
            return this;
        }

        public void truncate(int newLength) {
            data.truncate(newLength);
        }
    }
}
