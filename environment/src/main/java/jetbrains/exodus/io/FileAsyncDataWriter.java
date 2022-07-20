/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class FileAsyncDataWriter extends AbstractDataWriter implements AsyncDataWriter {
    private static final String DELETED_FILE_EXTENSION = ".del";
    private static final Logger logger = LoggerFactory.getLogger(FileAsyncDataWriter.class);

    private final FileDataReader reader;
    private FileDataReader.FileBlock block;
    private final LockingManager lockingManager;
    private AsynchronousFileChannel channel;
    private long position;


    public FileAsyncDataWriter(FileDataReader reader, String lockId) {
        this.reader = reader;
        lockingManager = new LockingManager(reader.getDir(), lockId);
    }

    private AsynchronousFileChannel openOrCreateFile(FileDataReader.FileBlock block, long length) throws IOException {
        try (var file = new RandomAccessFile(block, "rw")) {
            if (file.length() != length) {
                file.setLength(length);
                file.getChannel().force(true);
            }
        }

        AsynchronousFileChannel channel = AsynchronousFileChannel.open(block.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);

        this.block = block;
        this.channel = channel;
        this.position = length;

        return channel;
    }

    private AsynchronousFileChannel ensureFile(String errorMessage) {
        if (channel != null && channel.isOpen()) {
            return channel;
        }

        if (block != null) {
            try {
                return openOrCreateFile(block, block.length());
            } catch (IOException e) {
                throw ExodusException.toExodusException(e);
            }
        }

        throw new ExodusException(errorMessage);
    }

    @Override
    public Block write(byte[] b, int off, int len) {
        var channel = ensureFile("Can't write, FileDataWriter is closed");
        var buffer = ByteBuffer.wrap(b, off, len);
        writeBuffer(channel, buffer);

        return ensureBlock();
    }


    @Override
    public Block write(ByteBuffer b, int off, int len) {
        var channel = ensureFile("Can't write, FileDataWriter is closed");
        var buffer = b.slice(off, len);
        writeBuffer(channel, buffer);

        return ensureBlock();
    }

    @Override
    public Block write(@NotNull ByteBuffer[] buffers) {
        for (var buffer : buffers) {
            write(buffer, 0, buffer.limit());
        }

        return ensureBlock();
    }

    @Override
    public boolean lock(long timeout) {
        return lockingManager.lock(timeout);
    }

    @Override
    public boolean release() {
        return lockingManager.release();
    }

    @Override
    public String lockInfo() {
        return lockingManager.lockInfo();
    }

    @Override
    protected void syncImpl() {
        if (channel != null) {
            try {
                channel.force(true);
            } catch (IOException e) {
                throw ExodusException.toExodusException(e);
            }
        }
    }

    @Override
    protected void closeImpl() {
        try {
            ensureFile("Can't close already closed FileDataWriter").close();
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }

        channel = null;
        block = null;
    }

    @Override
    protected void clearImpl() {
        for (var file : LogUtil.listFiles(reader.getDir())) {
            if (!file.canWrite()) {
                setWritable(file);
            }

            if (file.exists() && !file.delete()) {
                throw new ExodusException("Failed to delete " + file);
            }
        }
    }

    @Override
    public void syncDirectory() {
    }

    @Override
    public void removeBlock(long blockAddress, @NotNull RemoveBlockType rbt) {
        var file = new FileDataReader.FileBlock(blockAddress, reader);
        removeFileFromFileCache(file);
        setWritable(file);
        boolean deleted;

        if (rbt == RemoveBlockType.Delete) {
            deleted = file.delete();
        } else {
            deleted = renameFile(file);
        }

        if (!deleted) {
            throw new ExodusException("Failed to delete " + file.getAbsolutePath());
        } else {
            if (logger.isDebugEnabled()) {
                logger.info("Deleted file " + file.getAbsolutePath());
            }
        }
    }

    @Override
    public void truncateBlock(long blockAddress, long length) {
        var file = new FileDataReader.FileBlock(blockAddress, reader);
        removeFileFromFileCache(file);
        setWritable(file);

        try (var rf = new RandomAccessFile(file, "rw")) {
            rf.setLength(length);
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }

        if (logger.isInfoEnabled()) {
            logger.info("Truncated file " + file.getAbsolutePath() + " to length = " + length);
        }
    }

    private boolean renameFile(File file) {
        var name = file.getName();
        return file.renameTo(new File(file.getParentFile(), name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION));
    }

    private void removeFileFromFileCache(File file) {
        try {
            SharedOpenFilesCache.getInstance().removeFile(file);
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }
    }

    private void writeBuffer(AsynchronousFileChannel channel, ByteBuffer buffer) {
        long position = this.position;
        while (buffer.remaining() > 0) {
            try {
                channel.write(buffer, position + buffer.position()).get();
            } catch (InterruptedException | ExecutionException e) {
                throw ExodusException.toExodusException(e);
            }
        }

        this.position += buffer.limit();
    }

    private Block ensureBlock() {
        if (block == null) {
            throw new ExodusException("Can't write, FileDataWriter is closed");
        }

        return block;
    }

    @Override
    public Block openOrCreateBlockImpl(long address, long length) {
        var block = new FileDataReader.FileBlock(address, reader);
        if (!block.canWrite()) {
            setWritable(block);
        }

        try {
            //noinspection resource
            openOrCreateFile(block, length);
        } catch (IOException e) {
            throw ExodusException.toExodusException(e);
        }

        return block;
    }

    private void setWritable(File file) {
        if (file.exists() && !file.setWritable(true)) {
            throw new ExodusException("Failed to set writable " + file.getAbsolutePath());
        }
    }

    @Override
    public CompletableFuture<Block> writeAsync(ByteBuffer b, int off, int len) {
        final CompletableFuture<Block> completableFuture = new CompletableFuture<>();

        var channel = ensureFile("Can't write, FileDataWriter is closed");
        var buffer = b.slice(off, len);

        var completionHandler = new FileWriteCompletionHandler(buffer, completableFuture, ensureBlock(), channel,
                position);
        channel.write(buffer, position, null, completionHandler);
        position += len;

        return completableFuture;
    }

    @SuppressWarnings("ClassCanBeRecord")
    private static final class FileWriteCompletionHandler implements CompletionHandler<Integer, Void> {
        private final ByteBuffer buffer;
        private final CompletableFuture<Block> completableFuture;
        private final Block block;
        private final AsynchronousFileChannel channel;
        private final long startPosition;


        private FileWriteCompletionHandler(ByteBuffer buffer, CompletableFuture<Block> completableFuture,
                                           Block block, AsynchronousFileChannel channel, long startPosition) {
            this.buffer = buffer;
            this.completableFuture = completableFuture;
            this.block = block;
            this.channel = channel;
            this.startPosition = startPosition;
        }

        @Override
        public void completed(Integer result, Void attachment) {
            if (buffer.remaining() > 0) {
                channel.write(buffer, startPosition + buffer.position(), null, this);
            } else {
                completableFuture.complete(block);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            completableFuture.completeExceptionally(exc);
        }
    }
}
