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

public final class NullLoggable extends RandomAccessLoggableImpl {

    public static final byte TYPE = 0;

    private static final NullLoggable PROTOTYPE = new NullLoggable(Loggable.NULL_ADDRESS, Loggable.NULL_ADDRESS);

    NullLoggable(final long address, final long end) {
        super(address, end, 1, TYPE, ByteIterableWithAddress.getEmpty(address + 1),
                0, NO_STRUCTURE_ID, true);
    }

    public static NullLoggable create() {
        return PROTOTYPE;
    }

    @Override
    public long getAddress() {
        return Loggable.NULL_ADDRESS;
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
}
