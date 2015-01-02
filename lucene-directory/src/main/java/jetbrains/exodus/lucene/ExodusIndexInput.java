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
package jetbrains.exodus.lucene;

import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.vfs.File;
import jetbrains.exodus.vfs.VfsInputStream;
import jetbrains.exodus.vfs.VirtualFileSystem;
import org.apache.lucene.store.IndexInput;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

@SuppressWarnings("CloneableClassInSecureContext")
public class ExodusIndexInput extends IndexInput {

    @NotNull
    private final VirtualFileSystem vfs;
    @NotNull
    private final File file;
    @NotNull
    private final Transaction txn;
    @NotNull
    private VfsInputStream input;
    private long currentPosition;

    public ExodusIndexInput(@NotNull final VirtualFileSystem vfs,
                            @NotNull final Transaction txn,
                            @NotNull final File file) {
        super("ExodusDirectory IndexInput for " + file.getPath());
        this.vfs = vfs;
        this.file = file;
        this.txn = txn;
        input = vfs.readFile(txn, file);
        currentPosition = 0;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public long getFilePointer() {
        return currentPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != currentPosition) {
            if (pos > currentPosition) {
                final long bytesToSkip = pos - currentPosition;
                if (input.skip(bytesToSkip) == bytesToSkip) {
                    currentPosition = pos;
                    return;
                }
            }
            input.close();
            input = vfs.readFile(txn, file, pos);
            currentPosition = pos;
        }
    }

    @Override
    public long length() {
        return vfs.getFileLength(txn, file);
    }

    @Override
    public byte readByte() throws IOException {
        ++currentPosition;
        return (byte) input.read();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        currentPosition += input.read(b, offset, len);
    }

    @SuppressWarnings("CloneCallsConstructors")
    @Override
    public final Object clone() {
        final ExodusIndexInput clone = (ExodusIndexInput) super.clone();
        clone.input = vfs.readFile(txn, file, currentPosition);
        return clone;
    }
}
