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

package jetbrains.exodus.log.replication

import mu.KLogging
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.utils.FunctionalUtils
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

internal class ByteBufferAsyncResponseHandler<R>(val output: ByteBuffer) : AsyncResponseTransformer<R, Int> {
    companion object : KLogging()

    val written = AtomicInteger()

    @Volatile
    private var response: R? = null

    override fun responseReceived(response: R) {
        this.response = response
    }

    override fun onStream(publisher: Publisher<ByteBuffer>) {
        written.set(0)
        publisher.subscribe(ByteBufferSubscriber())
    }

    override fun exceptionOccurred(throwable: Throwable) {
    }

    override fun complete() = written.get()

    private inner class ByteBufferSubscriber : Subscriber<ByteBuffer> {
        private lateinit var subscription: Subscription

        override fun onSubscribe(s: Subscription) {
            subscription = s
            s.request(1)
        }

        override fun onNext(byteBuffer: ByteBuffer) {
            val length = byteBuffer.remaining()
            val prevLength = written.getAndAdd(length)
            val outputLength = output.limit()
            if (prevLength <= outputLength) {
                val maxLength = minOf(outputLength - prevLength, length)
                FunctionalUtils.invokeSafely {
                    output.put(prevLength, byteBuffer, byteBuffer.remaining(), maxLength)
                }

            } else {
                logger.warn { "Data buffer overrun, drop ${prevLength - outputLength} bytes" }
            }
            subscription.request(1)
        }

        override fun onError(throwable: Throwable) {}

        override fun onComplete() {}
    }
}