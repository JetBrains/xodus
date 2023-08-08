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
import jetbrains.exodus.core.dataStructures.LongIntPair;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.io.AbstractDataWriter;
import jetbrains.exodus.io.Block;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public final class MemoryDataWriter extends AbstractDataWriter {
    private static final Logger logger = LoggerFactory.getLogger(MemoryDataWriter.class);
    private final Memory memory;

    private boolean closed;
    private Memory.Block data;

    public MemoryDataWriter(Memory memory) {
        this.memory = memory;
    }

    @Override
    public Block write(byte[] b, int off, int len) {
        checkClosed();
        data.write(b, off, len);
        return data;
    }

    @Override
    public void removeBlock(long blockAddress, @NotNull RemoveBlockType rbt) {
        if (!memory.removeBlock(blockAddress)) {
            throw new ExodusException("There is no memory block by address " + blockAddress);
        }

        logger.info("Deleted file {}", LogUtil.getLogFilename(blockAddress));
    }

    @Override
    public boolean lock(long timeout) {
        return true;
    }

    @Override
    public boolean release() {
        return true;
    }

    @Override
    public String lockInfo() {
        return null;
    }

    @Override
    public Pair<Block, CompletableFuture<LongIntPair>> asyncWrite(byte[] b, int off, int len) {
        var position = data.getSize();
        write(b, off, len);

        return new Pair<>(data, CompletableFuture.completedFuture(new LongIntPair(data.getAddress() + position, len)));
    }

    @Override
    public long position() {
        return data.getSize();
    }

    @Override
    public void syncImpl() {
    }

    @Override
    public void closeImpl() {
        closed = true;
    }

    @Override
    public void clearImpl() {
        memory.clear();
    }

    public Block openOrCreateBlockImpl(long address, long length) {
        var result = memory.getOrCreateBlockData(address, length);
        data = result;
        closed = false;
        return result;
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
    }
}
