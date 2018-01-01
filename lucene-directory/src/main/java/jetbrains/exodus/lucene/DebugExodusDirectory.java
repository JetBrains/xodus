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
package jetbrains.exodus.lucene;

import jetbrains.exodus.env.ContextualEnvironment;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.vfs.VfsConfig;
import org.apache.lucene.store.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;

public class DebugExodusDirectory extends Directory {

    private final ExodusDirectory directory;
    private final RAMDirectory debugDirectory;

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env) throws IOException {
        this(env, new SingleInstanceLockFactory());
    }

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env,
                                @NotNull final LockFactory lockFactory) throws IOException {
        this(env, StoreConfig.WITH_DUPLICATES, lockFactory);
    }

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env,
                                @NotNull final StoreConfig contentsStoreConfig,
                                @NotNull final LockFactory lockFactory) throws IOException {
        directory = new ExodusDirectory(env, contentsStoreConfig, lockFactory);
        debugDirectory = new RAMDirectory(directory);
        setLockFactory(lockFactory);
    }

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env,
                                @NotNull final VfsConfig vfsConfig,
                                @NotNull final StoreConfig contentsStoreConfig,
                                @NotNull final LockFactory lockFactory) throws IOException {
        directory = new ExodusDirectory(env, vfsConfig, contentsStoreConfig, lockFactory);
        debugDirectory = new RAMDirectory(directory);
        setLockFactory(lockFactory);
    }

    @Override
    public String[] listAll() throws IOException {
        final String[] result = directory.listAll();
        final String[] debugResult = debugDirectory.listAll();
        if (result.length != debugResult.length) {
            throwDebugMismatch();
        }
        Arrays.sort(result);
        Arrays.sort(debugResult);
        int i = 0;
        for (String file : debugResult) {
            if (!file.equals(result[i++])) {
                throwDebugMismatch();
            }
        }
        return result;
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        final boolean result = directory.fileExists(name);
        if (result != debugDirectory.fileExists(name)) {
            throwDebugMismatch();
        }
        return result;
    }

    @Override
    public long fileModified(String name) throws IOException {
        final long result = directory.fileModified(name);
        if (result != debugDirectory.fileModified(name)) {
            throwDebugMismatch();
        }
        return result;
    }

    @Override
    public void touchFile(String name) throws IOException {
        directory.touchFile(name);
        //noinspection deprecation
        debugDirectory.touchFile(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        directory.deleteFile(name);
        debugDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        final long result = directory.fileLength(name);
        if (result != debugDirectory.fileLength(name)) {
            throwDebugMismatch();
        }
        return result;
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return new DebugIndexOutput(name);
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        return new DebugIndexInput(name);
    }

    @Override
    public void close() throws IOException {
        directory.close();
        debugDirectory.close();
    }

    private static void throwDebugMismatch() {
        throw new RuntimeException("Debug directory mismatch");
    }

    private class DebugIndexOutput extends IndexOutput {

        private final IndexOutput output;
        private final IndexOutput debugOutput;

        private DebugIndexOutput(String name) throws IOException {
            output = directory.createOutput(name);
            debugOutput = debugDirectory.createOutput(name);
        }

        @Override
        public void flush() throws IOException {
            output.flush();
            debugOutput.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
            debugOutput.close();
        }

        @Override
        public long getFilePointer() {
            final long result = output.getFilePointer();
            if (result != debugOutput.getFilePointer()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public void seek(long pos) throws IOException {
            output.seek(pos);
            debugOutput.seek(pos);
        }

        @Override
        public long length() throws IOException {
            final long result = output.length();
            if (result != debugOutput.length()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            output.writeByte(b);
            debugOutput.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            output.writeBytes(b, offset, length);
            debugOutput.writeBytes(b, offset, length);
        }
    }

    @SuppressWarnings("CloneableClassInSecureContext")
    private class DebugIndexInput extends IndexInput {

        private IndexInput input;
        private IndexInput debugInput;

        private DebugIndexInput(String name) throws IOException {
            super("DebugIndexInput for " + name);
            input = directory.openInput(name);
            debugInput = debugDirectory.openInput(name);
        }

        @Override
        public void close() throws IOException {
            input.close();
            debugInput.close();
        }

        @Override
        public long getFilePointer() {
            final long result = input.getFilePointer();
            if (result != debugInput.getFilePointer()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public void seek(long pos) throws IOException {
            input.seek(pos);
            debugInput.seek(pos);
        }

        @Override
        public long length() {
            final long result = input.length();
            if (result != debugInput.length()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public byte readByte() throws IOException {
            final byte result = input.readByte();
            if (result != debugInput.readByte()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            final long before = getFilePointer();
            input.readBytes(b, offset, len);
            final byte[] bytes = new byte[len];
            debugInput.readBytes(bytes, 0, len);
            final long after = getFilePointer();
            for (int i = 0; i < (int) (after - before); ++i) {
                if (bytes[i] != b[offset + i]) {
                    throwDebugMismatch();
                }
            }
        }

        @Override
        public final Object clone() {
            final DebugIndexInput result = (DebugIndexInput) super.clone();
            result.input = (IndexInput) input.clone();
            result.debugInput = (IndexInput) debugInput.clone();
            return result;
        }
    }
}
