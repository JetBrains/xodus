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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class FileAsyncHandler(private val path: Path, private val lastPageStart: Long, private val lastPage: ByteArray?) : AsyncResponseHandler<GetObjectResponse, WriteResult> {
    private lateinit var fileChannel: AsynchronousFileChannel
    @Volatile
    private var response: GetObjectResponse? = null
    private val lastPageWritten = AtomicInteger()

    override fun responseReceived(response: GetObjectResponse) {
        this.response = response
    }

    override fun onStream(publisher: Publisher<ByteBuffer>) {
        lastPageWritten.set(0)
        fileChannel = invokeSafely<AsynchronousFileChannel> { open(path) }
        publisher.subscribe(FileSubscriber())
    }

    override fun exceptionOccurred(throwable: Throwable) {
        try {
            invokeSafely { fileChannel.close() }
        } finally {
            invokeSafely { Files.delete(path) }
        }
    }

    override fun complete(): WriteResult? {
        if (::fileChannel.isInitialized) {
            invokeSafely { fileChannel.close() }
        }
        return response?.let {
            WriteResult(it.contentLength(), lastPageWritten.get())
        }
    }


    private fun open(path: Path): AsynchronousFileChannel {
        return AsynchronousFileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    }

    private fun ByteBuffer.copyBytes(output: ByteArray, offset: Int, length: Int) {
        if (hasArray()) {
            System.arraycopy(
                    array(),
                    arrayOffset() + position(),
                    output,
                    offset,
                    length
            )
        }

        asReadOnlyBuffer().get(output, offset, length)
    }

    private inner class FileSubscriber : Subscriber<ByteBuffer> {

        @Volatile
        private var writeInProgress = false
        @Volatile
        private var closeOnLastWrite = false
        private val position = AtomicLong()
        private lateinit var subscription: Subscription

        override fun onSubscribe(s: Subscription) {
            this.subscription = s
            s.request(1)
        }

        override fun onNext(byteBuffer: ByteBuffer) {
            writeInProgress = true
            fileChannel.write(byteBuffer, position.get(), byteBuffer, object : CompletionHandler<Int, ByteBuffer> {
                override fun completed(result: Int, attachment: ByteBuffer) {
                    if (result > 0) {
                        val writtenLength = result.toLong()
                        updateLastPage(writtenLength, attachment)
                        synchronized(this@FileSubscriber) {
                            // TODO: this can be replaced by AtomicInteger FSM
                            if (closeOnLastWrite) {
                                close()
                            } else {
                                subscription.request(1)
                            }
                            writeInProgress = false
                        }
                    }
                }

                override fun failed(exc: Throwable, attachment: ByteBuffer) {
                    subscription.cancel()
                }
            })

        }

        override fun onError(t: Throwable) {
            // Error handled by response handler
        }

        override fun onComplete() {
            synchronized(this) {
                if (writeInProgress) {
                    closeOnLastWrite = true
                } else {
                    close()
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
                val offset = maxOf(0, startPosition - lastPageStart).toInt()
                val length = (minOf(lastPage.size.toLong(), writtenLength) - offset).toInt()
                attachment.copyBytes(lastPage, offset, length)
                lastPageWritten.addAndGet(length)
            }
        }

        private fun close() {
            if (::fileChannel.isInitialized) {
                invokeSafely { fileChannel.close() }
            }
        }

        override fun toString(): String {
            return "$javaClass:$path"
        }
    }
}
