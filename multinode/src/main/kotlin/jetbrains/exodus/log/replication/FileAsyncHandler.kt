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
package jetbrains.exodus.log.replication

import jetbrains.exodus.log.BufferedDataWriter
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncResponseHandler
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.utils.FunctionalUtils.invokeSafely
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

// async handler which writes data to the filesystem and copies last written bytes to "last page" if provided
// performs fsync on file end
class FileAsyncHandler(
        private val path: Path,
        private val startingLength: Long,
        private val lastPageStart: Long = 0,
        private val lastPage: BufferedDataWriter.MutablePage? = null
) : AsyncResponseHandler<GetObjectResponse, WriteResult> {
    private val lastPageStartingLength = lastPage?.count ?: 0

    private lateinit var fileChannel: AsynchronousFileChannel
    @Volatile
    private var response: GetObjectResponse? = null
    private val lastPageLength = AtomicInteger()
    private val writeInProgressLock = Semaphore(1)
    @Volatile
    private var error: Throwable? = null

    override fun responseReceived(response: GetObjectResponse) {
        this.response = response
    }

    override fun onStream(publisher: Publisher<ByteBuffer>) {
        lastPageLength.set(lastPageStartingLength)
        fileChannel = invokeSafely<AsynchronousFileChannel> { open(path) }
        publisher.subscribe(FileSubscriber())
    }

    override fun exceptionOccurred(throwable: Throwable) {
        try {
            writeInProgressLock.acquire()
            error = throwable
            close()
        } finally {
            writeInProgressLock.release()
            invokeSafely { Files.delete(path) }
        }
    }

    override fun complete(): WriteResult {
        writeInProgressLock.acquire()
        error?.let { throw it }
        return response?.let {
            WriteResult(it.contentLength(), lastPageLength.get())
        } ?: throw IllegalStateException("Response not set")
    }

    private fun open(path: Path): AsynchronousFileChannel {
        return AsynchronousFileChannel.open(path, StandardOpenOption.WRITE, if (startingLength > 0) {
            StandardOpenOption.WRITE
        } else {
            StandardOpenOption.CREATE_NEW
        })
    }

    private fun close() {
        if (::fileChannel.isInitialized) {
            fileChannel.force(false)
            invokeSafely { fileChannel.close() }
        }
    }

    private inner class FileSubscriber : Subscriber<ByteBuffer> {

        @Volatile
        private var closeOnLastWrite = false
        private val position = AtomicLong(startingLength)
        private lateinit var subscription: Subscription

        override fun onSubscribe(s: Subscription) {
            this.subscription = s
            s.request(1)
        }

        override fun onNext(byteBuffer: ByteBuffer) {
            // TODO: try to replace lock with AtomicInteger FSM
            writeInProgressLock.acquire()
            fileChannel.write(byteBuffer, position.get(), byteBuffer, object : CompletionHandler<Int, ByteBuffer> {
                override fun completed(result: Int, attachment: ByteBuffer) {
                    try {
                        if (result > 0) {
                            val writtenLength = result.toLong()
                            updateLastPage(writtenLength, attachment)
                            if (closeOnLastWrite) {
                                close()
                            } else {
                                subscription.request(1)
                            }
                        }
                    } catch (t: Throwable) {
                        error = t
                        subscription.cancel()
                    } finally {
                        writeInProgressLock.release()
                    }
                }

                override fun failed(exc: Throwable, attachment: ByteBuffer) {
                    error = exc
                    subscription.cancel()
                    close()
                }
            })

        }

        override fun onError(t: Throwable) {
            // Error handled by response handler
        }

        override fun onComplete() {
            if (!writeInProgressLock.tryAcquire()) {
                closeOnLastWrite = true
            } else {
                try {
                    close()
                } finally {
                    writeInProgressLock.release()
                }
            }
        }

        private fun updateLastPage(writtenLength: Long, attachment: ByteBuffer) {
            val endPosition = position.addAndGet(writtenLength)
            if (lastPage != null && endPosition >= lastPageStart) {
                attachment.flip()
                if (writtenLength > attachment.limit()) {
                    throw IllegalStateException("Unexpected buffer state")
                }
                val startPosition = endPosition - writtenLength
                val skip: Int
                val offset: Int
                if (startPosition < lastPageStart) {
                    skip = (lastPageStart - startPosition).toInt()
                    offset = 0
                } else {
                    offset = (startPosition - lastPageStart).toInt()
                    skip = 0
                }
                val bytes = lastPage.bytes
                val length = minOf(bytes.size.toLong() - offset, writtenLength - skip).toInt()
                attachment.copyBytes(skip, bytes, offset, length)
                lastPageLength.addAndGet(length)
            }
        }

        override fun toString(): String {
            return "$javaClass:$path"
        }
    }
}
