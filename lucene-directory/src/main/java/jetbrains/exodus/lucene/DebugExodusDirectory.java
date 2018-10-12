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
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class DebugExodusDirectory extends Directory {

    private final ExodusDirectory directory;
    private final RAMDirectory debugDirectory;

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env) {
        this(env, StoreConfig.WITH_DUPLICATES);
    }

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env,
                                @NotNull final StoreConfig contentsStoreConfig) {
        directory = new ExodusDirectory(env, contentsStoreConfig);
        debugDirectory = new RAMDirectory();
    }

    public DebugExodusDirectory(@NotNull final ContextualEnvironment env,
                                @NotNull final VfsConfig vfsConfig,
                                @NotNull final StoreConfig contentsStoreConfig) {
        directory = new ExodusDirectory(env, vfsConfig, contentsStoreConfig);
        debugDirectory = new RAMDirectory();
    }

    @Override
    public String[] listAll() {
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
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new DebugIndexOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return createOutput(IndexFileNames.segmentFileName(prefix, suffix + '_' + directory.nextTicks(), "tmp"), context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        directory.sync(names);
        debugDirectory.sync(names);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        directory.rename(source, dest);
        debugDirectory.rename(source, dest);
    }

    @Override
    public void syncMetaData() throws IOException {
        directory.syncMetaData();
        debugDirectory.syncMetaData();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new DebugIndexInput(name, context);
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return debugDirectory.obtainLock(name);
    }

    @Override
    public void close() {
        directory.close();
        debugDirectory.close();
    }

    private static void throwDebugMismatch() {
        throw new RuntimeException("Debug directory mismatch");
    }

    private class DebugIndexOutput extends IndexOutput {

        private final IndexOutput output;
        private final IndexOutput debugOutput;

        private DebugIndexOutput(String name, IOContext context) throws IOException {
            super("DebugIndexOutput for " + name, name);
            output = directory.createOutput(name, context);
            debugOutput = debugDirectory.createOutput(name, context);
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
        public long getChecksum() throws IOException {
            final long result = output.getChecksum();
            if (result != debugOutput.getChecksum()) {
                throwDebugMismatch();
            }
            return result;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            output.writeByte(b);
            debugOutput.writeByte(b);
            getFilePointer();
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            output.writeBytes(b, offset, length);
            debugOutput.writeBytes(b, offset, length);
            getFilePointer();
        }
    }

    @SuppressWarnings("CloneableClassInSecureContext")
    private class DebugIndexInput extends IndexInput {

        private String name;
        private IndexInput input;
        private IndexInput debugInput;

        private DebugIndexInput(String name, IOContext context) throws IOException {
            this(name, context, 0L);
        }

        private DebugIndexInput(String name, IOContext context, long position) throws IOException {
            super("DebugIndexInput for " + name);
            this.name = name;
            input = directory.openInput(name, context);
            debugInput = debugDirectory.openInput(name, context);
            if (position > 0) {
                input.seek(position);
                debugInput.seek(position);
            }
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
            if (input.getFilePointer() != debugInput.getFilePointer()) {
                throwDebugMismatch();
            }
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
            final byte debugByte = debugInput.readByte();
            if (result != debugByte) {
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
        public final IndexInput clone() {
            final DebugIndexInput result = (DebugIndexInput) super.clone();
            result.name = name;
            result.input = input.clone();
            result.debugInput = debugInput.clone();
            return result;
        }

        @Override
        public IndexInput slice(String sliceDescription, final long offset, final long length) throws IOException {
            final DebugIndexInput result = (DebugIndexInput) super.clone();
            result.name = name;
            result.input = input.slice(sliceDescription, offset, length);
            result.debugInput = debugInput.slice(sliceDescription, offset, length);
            return result;
        }
    }
}
