/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.log;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.io.Block;

import java.nio.ByteBuffer;

public interface BufferedDataWriter {
    BlockSet.Mutable getBlockSetMutable();

    void setHighAddress(long highAddress);

    MutablePage allocLastPage(long pageAddress);

    void write(byte b);

    void write(ByteBuffer b, int len) throws ExodusException;

    void flush();

    Block openOrCreateBlock(long address, long length);

    void setLastPageLength(int lastPageLength);

    int getLastPageLength();

    long getLastWrittenFileLength(long fileLengthBound);

    LogTip getStartingTip();

    void incHighAddress(long delta);

    long getHighAddress();

    LogTip getUpdatedTip();

    byte getByte(long address, byte max);
}
