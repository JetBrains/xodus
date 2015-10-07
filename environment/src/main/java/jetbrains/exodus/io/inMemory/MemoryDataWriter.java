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

import jetbrains.exodus.io.AbstractDataWriter;
import org.jetbrains.annotations.NotNull;

public class MemoryDataWriter extends AbstractDataWriter {

    @NotNull
    private final Memory memory;
    private Memory.Block data;

    public MemoryDataWriter(@NotNull Memory memory) {
        data = null;
        this.memory = memory;
    }

    @Override
    public boolean write(byte[] b, int off, int len) {
        data.write(b, off, len);
        return true;
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
    protected void syncImpl() {
    }

    @Override
    protected void closeImpl() {
        data = null;
    }

    @Override
    protected void openOrCreateBlockImpl(final long address, final long length) {
        data = memory.getOrCreateBlockData(address, length);
    }

}
