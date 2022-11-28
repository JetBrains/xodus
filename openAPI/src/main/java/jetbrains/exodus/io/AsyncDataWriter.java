package jetbrains.exodus.io;

import jetbrains.exodus.core.dataStructures.Pair;

import java.util.concurrent.CompletableFuture;

/**
 * {@code AsyncDataWriter} allows write data to the {@code Log} using asynchronous IO.
 * Writer itself does not limit amount of simultaneous asynchronous requests and user
 * of this interface should track this limit on its own.
 *
 * This interface is not threadsafe and can not be used without external synchronization.
 *
 * @since 3.0
 */
public interface AsyncDataWriter extends DataWriter {
    /**
     * Asynchronously writes binary data to {@code Log}.
     * Returns new {@linkplain Block} instance representing mutable block and completable future which will be executed
     * once asynchronous write was executed.
     *
     * This method is not thread safe and can not be used by multiple threads without external synchronization.
     *
     * @param b   binary data array
     * @param off starting offset in the array
     * @param len number of byte to write
     * @return  Pair which consist of mutable {@linkplain Block} instance and completable future which services
     * as indicator of completion (successful or unsuccessful) of requested write.
     * @see Block
     */
    Pair<Block, CompletableFuture<Void>> asyncWrite(byte[] b, int off, int len);


    /**
     * Return current position of writer inside the file.
     * This method is not thread safe and can not be used without external synchronization.
     * @return current position of writer inside the file.
     */
    long position();
}
