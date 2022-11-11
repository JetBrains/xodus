/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log;

import org.jetbrains.annotations.NotNull;

public final class NullLoggable implements RandomAccessLoggable {

    public static final byte TYPE = 0;
    private final long address;
    private final long end;

    private static final NullLoggable PROTOTYPE = new NullLoggable(Loggable.NULL_ADDRESS, Loggable.NULL_ADDRESS);

    NullLoggable(final long address, final long end) {
        this.address = address;
        this.end = end;
    }

    public static NullLoggable create() {
        return PROTOTYPE;
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public byte getType() {
        return TYPE;
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public long end() {
        return end;
    }

    @Override
    public int getDataLength() {
        return 0;
    }

    @Override
    public int getStructureId() {
        return NO_STRUCTURE_ID;
    }

    @Override
    public boolean isDataInsideSinglePage() {
        return true;
    }

    public static boolean isNullLoggable(final byte type) {
        return type == TYPE;
    }

    public static boolean isNullLoggable(@NotNull final Loggable loggable) {
        return isNullLoggable(loggable.getType());
    }

    @Override
    public @NotNull ByteIterableWithAddress getData() {
        return ArrayByteIterableWithAddress.EMPTY;
    }
}
