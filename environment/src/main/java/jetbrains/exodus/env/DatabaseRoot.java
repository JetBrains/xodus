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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

final class DatabaseRoot {

    static final byte DATABASE_ROOT_TYPE = 1;

    private static final long MAGIC_DIFF = 199L;

    @NotNull
    private final Loggable loggable;
    private final long rootAddress;
    private final int lastStructureId;
    private final boolean isValid;

    DatabaseRoot(@NotNull final Loggable loggable) {
        this.loggable = loggable;
        final ByteIterator it = loggable.getData().iterator();
        rootAddress = CompressedUnsignedLongByteIterable.getLong(it);
        lastStructureId = CompressedUnsignedLongByteIterable.getInt(it);
        isValid = rootAddress ==
            CompressedUnsignedLongByteIterable.getLong(it) - lastStructureId - MAGIC_DIFF;
    }

    public long getAddress() {
        return loggable.getAddress();
    }

    public long length() {
        return loggable.length();
    }

    long getRootAddress() {
        return rootAddress;
    }

    int getLastStructureId() {
        return lastStructureId;
    }

    boolean isValid() {
        return isValid;
    }

    static ByteIterable asByteIterable(final long rootAddress, final int lastStructureId) {
        final LightOutputStream output = new LightOutputStream(20);
        CompressedUnsignedLongByteIterable.fillBytes(rootAddress, output);
        CompressedUnsignedLongByteIterable.fillBytes(lastStructureId, output);
        CompressedUnsignedLongByteIterable.fillBytes(rootAddress + lastStructureId + MAGIC_DIFF, output);
        return output.asArrayByteIterable();
    }
}
