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
package jetbrains.exodus.vfs;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;

/**
 * Describes a file of a {@linkplain VirtualFileSystem}. {@code File} is identified by its {@linkplain #getPath()}.
 * Path is an abstract string, it can be used to codify a hierarchy, though {@linkplain VirtualFileSystem} doesn't
 * contain methods to enumerate files by a path prefix.
 *
 * <p>In addition, {@code File} can be identified by a file descriptor which an internal {@linkplain VirtualFileSystem}
 * {@code long} unique id. Identifying by file descriptor allows to
 * {@linkplain VirtualFileSystem#renameFile(Transaction, File, String) rename} a file.
 *
 * <p>{@code File} has its {@linkplain #getCreated() created} and {@linkplain #getLastModified() last modified} time.
 *
 * @see VirtualFileSystem
 * @see VirtualFileSystem#createFile(Transaction, String)
 * @see VirtualFileSystem#openFile(Transaction, String, boolean)
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

    /**
     * Returns the path which the {@code File} is identified by. Path is an abstract string, it can be used to codify
     * a hierarchy, though {@linkplain VirtualFileSystem} doesn't contain methods to enumerate files by a path prefix.
     *
     * @return path of the {@code File}
     */
    @NotNull
    public String getPath() {
        return path;
    }

    /**
     * @return the {@code File}'s internal unique id
     */
    public long getDescriptor() {
        return fd;
    }

    /**
     * Returns the time when the {@code File} was created. This value never changes.
     *
     * @return the time when the {@code File} was created
     * @see VirtualFileSystem#createFile(Transaction, String)
     * @see VirtualFileSystem#createFile(Transaction, long, String)
     * @see VirtualFileSystem#openFile(Transaction, String, boolean)
     */
    public long getCreated() {
        return created;
    }

    /**
     * Returns the time when the {@code File} was modified last time or explicitly touched.
     * {@linkplain VirtualFileSystem}'s methods that open {@linkplain OutputStream} by file descriptor, not
     * {@linkplain File} instance, return the stream which doesn't update last modified time.
     *
     * <p>The returned value is actual to the moment of the {@code File} instance creation,
     * {@linkplain VirtualFileSystem#openFile(Transaction, String, boolean) open} new {@code File} instance to get
     * consistent value of last modified time.
     *
     * @return the time when the {@code File} was modified last time or explicitly touched
     * @see VirtualFileSystem#touchFile(Transaction, File)
     */
    public long getLastModified() {
        return lastModified;
    }

    @Override
    public String toString() {
        return path + "[fd = " + fd + ']';
    }

    ArrayByteIterable toByteIterable() {
        final LightOutputStream output = new LightOutputStream();
        final int[] bytes = new int[8];
        LongBinding.writeCompressed(output, fd, bytes);
        LongBinding.writeCompressed(output, created, bytes);
        LongBinding.writeCompressed(output, lastModified, bytes);
        return output.asArrayByteIterable();
    }
}
