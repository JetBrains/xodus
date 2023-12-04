/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.core.dataStructures.LongIntPair;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.system.JVMConstants;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AsyncFileDataWriter extends AbstractDataWriter {
    public static final String DELETED_FILE_EXTENSION = ".del";
    private static final Logger logger = LoggerFactory.getLogger(AsyncFileDataWriter.class);

    private AsynchronousFileChannel dirChannel;
    private final FileDataReader reader;
    private AsynchronousFileChannel channel;
    private FileDataReader.FileBlock block;

    private final LockingManager lockingManager;
    private long position;

    public AsyncFileDataWriter(final FileDataReader reader) {
        this(reader, null);
    }

    public AsyncFileDataWriter(final FileDataReader reader, final String lockId) {
        this.reader = reader;

        this.lockingManager = new LockingManager(reader.getDir(), lockId);
        AsynchronousFileChannel channel = null;

        if (!JVMConstants.getIS_ANDROID()) {
            try {
                channel = AsynchronousFileChannel.open(reader.getDir().toPath());
                // try to force as XD-698 requires
                channel.force(false);
            } catch (IOException e) {
                channel = null;
                warnCantFsyncDirectory();
            }
        }

        dirChannel = channel;
    }


    @Override
    public Block write(byte[] b, int off, int len) {
        var pair = asyncWrite(b, off, len);
        try {
            pair.getSecond().get();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                if (lockingManager.getUsableSpace() < len) {
                    throw new OutOfDiskSpaceException(e);
                }
            }

            throw new ExodusException("Can not write into file.", e);
        }

        return block;
    }

    @Override
    public Pair<Block, CompletableFuture<LongIntPair>> asyncWrite(byte[] b, int off, int len) {
        AsynchronousFileChannel channel;
        try {
            channel = ensureChannel("Can't write, AsyncFileDataWriter is closed");

        } catch (IOException e) {
            if (lockingManager.getUsableSpace() < len) {
                throw new OutOfDiskSpaceException(e);
            }

            throw new ExodusException("Can not write into file.", e);
        }
        var buffer = ByteBuffer.wrap(b, off, len);

        var future = new CompletableFuture<LongIntPair>();
        channel.write(buffer, position, null, new WriteCompletionHandler(buffer, future,
                lockingManager, channel, position, block.getAddress(), len));
        position += len;

        return new Pair<>(block, future);
    }

    @Override
    public long position() {
        return position;
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
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new ExodusException("Can not synchronize file " + block.getAbsolutePath(), e);
        }
    }

    @Override
    protected void closeImpl() {
        try {
            ensureChannel("Can't close already closed " + AsyncFileDataWriter.class.getName()).close();
            if (dirChannel != null) {
                dirChannel.force(false);
            }

            this.channel = null;
            this.dirChannel = null;
            this.block = null;
        } catch (IOException e) {
            throw new ExodusException("Can not close file " + block.getAbsolutePath(), e);
        }
    }

    @Override
    protected void clearImpl() {
        for (var file : LogUtil.listFiles(reader.getDir())) {
            if (!file.canWrite()) {
                if (!file.setWritable(true)) {
                    throw new ExodusException("File " + file.getAbsolutePath() + " is protected from writes.");
                }
            }

            if (file.exists() && !file.delete()) {
                throw new ExodusException("Failed to delete " + file.getAbsolutePath());
            }
        }
    }

    @Override
    protected Block openOrCreateBlockImpl(long address, long length) {
        var block = new FileDataReader.FileBlock(address, reader);
        try {
            //noinspection resource
            openOrCreateChannel(block, length);
        } catch (IOException e) {
            throw new ExodusException("Channel can not be created for the file " + block.getAbsolutePath(), e);
        }
        return block;
    }

    @Override
    public void syncDirectory() {
        try {
            if (dirChannel != null) {
                dirChannel.force(false);
            }
        } catch (IOException e) {
            warnCantFsyncDirectory();
        }
    }

    @Override
    public void removeBlock(long blockAddress, @NotNull RemoveBlockType rbt) {
        var block = new FileDataReader.FileBlock(blockAddress, reader);
        removeFileFromFileCache(block);

        if (block.exists() && !block.setWritable(true)) {
            throw new ExodusException("File " + block.getAbsolutePath() + " is protected from write.");
        }

        boolean deleted;
        if (rbt == RemoveBlockType.Delete) {
            deleted = block.delete();
        } else {
            deleted = renameFile(block);
        }

        if (!deleted) {
            throw new ExodusException("Failed to delete file " + block.getAbsolutePath());
        } else {
            logger.info("Deleted file " + block.getAbsolutePath());
        }
    }

    public static boolean renameFile(File file) {
        var name = file.getName();
        return file.renameTo(
                new File(
                        file.getParentFile(),
                        name.substring(0, name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION
                )
        );
    }

    @Override
    @Deprecated
    public void truncateBlock(long blockAddress, long length) {
        var block = new FileDataReader.FileBlock(blockAddress, reader);
        removeFileFromFileCache(block);

        if (block.exists() && !block.setWritable(true)) {
            throw new ExodusException("File " + block.getAbsolutePath() + " is protected from write.");
        }

        try (var file = new RandomAccessFile(block, "rw")) {
            file.setLength(length);
        } catch (FileNotFoundException e) {
            throw new ExodusException("File " + block.getAbsolutePath() + " was not found", e);
        } catch (IOException e) {
            throw new ExodusException("Can not truncate file " + block.getAbsolutePath(), e);
        }

        logger.info("Truncated file " + block.getAbsolutePath() + " to length = " + length);
    }

    private static void removeFileFromFileCache(File file) {
        try {
            SharedOpenFilesCache.getInstance().removeFile(file);
        } catch (IOException e) {
            throw new ExodusException("Can not remove file " + file.getAbsolutePath(), e);
        }
    }

    private void warnCantFsyncDirectory() {
        this.dirChannel = null;
        logger.warn("Can't open directory channel. Log directory fsync won't be performed.");
    }

    private AsynchronousFileChannel ensureChannel(String errorMessage) throws IOException {
        if (channel != null) {
            if (channel.isOpen()) {
                return channel;
            }

            return openOrCreateChannel(block, Files.size(block.toPath()));
        }

        throw new ExodusException(errorMessage);
    }

    private AsynchronousFileChannel openOrCreateChannel(FileDataReader.FileBlock fileBlock,
                                                        long length) throws IOException {
        var blockPath = fileBlock.toPath();

        if (!fileBlock.exists()) {
            Files.createFile(blockPath);
        }
        if (!fileBlock.canWrite()) {
            if (!fileBlock.setWritable(true)) {
                throw new ExodusException("File " + fileBlock.getAbsolutePath() + " is protected from writes and can not be used.");
            }
        }

        var position = (Long) Files.getAttribute(blockPath, "size");

        if (position != length) {
            throw new ExodusException("Invalid size for the file " + blockPath.toAbsolutePath());
        }

        final AsynchronousFileChannel channel = AsynchronousFileChannel.open(blockPath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        this.block = fileBlock;
        this.channel = channel;
        this.position = position;

        return channel;
    }

    @NotNull
    public String lockFilePath() {
        return lockingManager.lockFilePath();
    }

    private static final class WriteCompletionHandler implements CompletionHandler<Integer, Void> {
        private final @NotNull ByteBuffer buffer;
        private final @NotNull CompletableFuture<LongIntPair> future;
        private final @NotNull LockingManager lockingManager;

        private final @NotNull AsynchronousFileChannel channel;

        private final long position;
        private final long address;

        private final int len;

        private WriteCompletionHandler(@NotNull ByteBuffer buffer,
                                       @NotNull CompletableFuture<LongIntPair> future,
                                       @NotNull LockingManager lockingManager,
                                       @NotNull AsynchronousFileChannel channel, final long position,
                                       final long address, int len) {
            this.buffer = buffer;
            this.future = future;
            this.lockingManager = lockingManager;
            this.channel = channel;
            this.position = position;
            this.address = address;
            this.len = len;
        }


        @Override
        public void completed(Integer result, Void attachment) {
            if (buffer.remaining() > 0) {
                channel.write(buffer, buffer.position() + position, null, this);
                return;
            }

            future.complete(new LongIntPair(address + position, len));
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (lockingManager.getUsableSpace() < len) {
                future.completeExceptionally(new OutOfDiskSpaceException(exc));
            } else {
                future.completeExceptionally(exc);
            }
        }
    }
}
