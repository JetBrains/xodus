/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.vfs;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 * Describes a file in a VirtualFileSystem. File never is a directory.
 */
public class File {

    @NotNull
    private final String path;
    private final long fd;
    private final long created;
    private final long lastModified;

    File(@NotNull final String path, final long fd, final long created, final long lastModified) {
        this.path = path;
        this.fd = fd;
        this.created = created;
        this.lastModified = lastModified;
    }

    File(@NotNull final File origin) {
        this(origin.path, origin.fd, origin.created, System.currentTimeMillis());
    }

    File(@NotNull final String path, @NotNull final ByteIterable iterable) {
        this.path = path;
        final ByteIterator iterator = iterable.iterator();
        fd = LongBinding.readCompressed(iterator);
        created = LongBinding.readCompressed(iterator);
        lastModified = LongBinding.readCompressed(iterator);
    }

    @NotNull
    public String getPath() {
        return path;
    }

    public long getDescriptor() {
        return fd;
    }

    public long getCreated() {
        return created;
    }

    public long getLastModified() {
        return lastModified;
    }

    @Override
    public String toString() {
        return path + "[fd = " + fd + ']';
    }

    ArrayByteIterable toByteIterable() {
        final LightOutputStream output = new LightOutputStream();
        LongBinding.writeCompressed(output, fd);
        LongBinding.writeCompressed(output, created);
        LongBinding.writeCompressed(output, lastModified);
        return output.asArrayByteIterable();
    }
}
