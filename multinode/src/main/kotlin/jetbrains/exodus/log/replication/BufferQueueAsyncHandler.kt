/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.log.replication

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.nio.ByteBuffer
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

// async handler which exposes a queue for sequential writing of response data on caller thread
class BufferQueueAsyncHandler : AsyncResponseTransformer<GetObjectResponse, GetObjectResponse> {
    companion object {
        private const val waitForSubscriptionInterval = 20L
        val finish: ByteBuffer = ByteBuffer.allocate(0)
    }

    private val subscriber = AtomicReference<QueueSubscriber>()
    private val subscriptionFuture = CompletableFuture<Subscription>()
    val queue = ArrayBlockingQueue<ByteBuffer>(2)

    @Volatile
    private var response: GetObjectResponse? = null

    fun waitForSubscription(forObject: CompletableFuture<GetObjectResponse>, maxMillis: Long = 30000): Subscription {
        var remaining = maxMillis
        while (remaining > 0) {
            try {
                return subscriptionFuture.get(waitForSubscriptionInterval, TimeUnit.MILLISECONDS)
            } catch (t: TimeoutException) {
                remaining -= waitForSubscriptionInterval
                if (forObject.isCompletedExceptionally) {
                    forObject.get()
                }
            }
        }
        throw TimeoutException()
    }

    override fun responseReceived(response: GetObjectResponse) {
        this.response = response
    }

    override fun exceptionOccurred(t: Throwable) {
        queue.add(finish)
        subscriptionFuture.completeExceptionally(t)
    }

    override fun onStream(publisher: Publisher<ByteBuffer>) {
        val sub = QueueSubscriber()
        if (subscriber.compareAndSet(null, sub)) {
            publisher.subscribe(sub)
        } else {
            IllegalStateException("Re-try is not supported").let {
                subscriptionFuture.completeExceptionally(it)
                throw it
            }
        }
    }

    override fun complete(): GetObjectResponse {
        return response ?: throw IllegalStateException("Not subscribed")
    }

    private inner class QueueSubscriber : Subscriber<ByteBuffer> {
        private lateinit var subscription: Subscription

        override fun onComplete() {
            queue.add(finish)
        }

        override fun onSubscribe(s: Subscription) {
            this.subscription = s
            subscriptionFuture.complete(s)
            s.request(1)
        }

        override fun onNext(t: ByteBuffer) {
            if (!queue.add(t)) {
                throw IllegalStateException("Unexpected capacity problems: subscription.request not invoked?")
            }
        }

        override fun onError(t: Throwable) {
            subscription.cancel()
            subscriptionFuture.completeExceptionally(t)
        }
    }
}
