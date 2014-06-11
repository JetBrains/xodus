/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import jetbrains.exodus.vfs.VirtualFileSystem;
import org.apache.lucene.store.IndexOutput;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class ExodusIndexOutput extends IndexOutput {

    @NotNull
    private final VirtualFileSystem vfs;
    @NotNull
    private final File file;
    @NotNull
    private final Transaction txn;
    @NotNull
    private OutputStream output;
    private long currentPosition;

    public ExodusIndexOutput(@NotNull final VirtualFileSystem vfs,
                             @NotNull final Transaction txn,
                             @NotNull final File file) {
        this.vfs = vfs;
        this.file = file;
        this.txn = txn;
        output = vfs.writeFile(txn, file);
        currentPosition = 0;
    }

    @Override
    public void flush() throws IOException {
        // nothing to do
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    @Override
    public long getFilePointer() {
        return currentPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != currentPosition) {
            output.close();
            output = vfs.writeFile(txn, file, pos);
            currentPosition = pos;
        }
    }

    @Override
    public long length() throws IOException {
        return vfs.getFileLength(txn, file);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        output.write(b);
        ++currentPosition;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        if (length > 0) {
            if (length == 1) {
                writeByte(b[offset]);
            } else {
                output.write(b, offset, length);
                currentPosition += length;
            }
        }
    }
}
