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
package jetbrains.exodus.io;

import jetbrains.exodus.util.SafeByteBufferCleaner;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

final class SharedMappedByteBuffer implements AutoCloseable {

    @NotNull
    private final ByteBuffer buffer;
    private final AtomicInteger clients;

    SharedMappedByteBuffer(@NotNull final SharedRandomAccessFile file) throws IOException {
        buffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0L, file.length());
        clients = new AtomicInteger();
    }

    void employ() {
        clients.incrementAndGet();
    }

    @NotNull
    ByteBuffer getBuffer() {
        return buffer.slice();
    }

    @Override
    public void close() {
        if (clients.decrementAndGet() < 0) {
            SafeByteBufferCleaner.INSTANCE.clean(buffer);
        }
    }
}
