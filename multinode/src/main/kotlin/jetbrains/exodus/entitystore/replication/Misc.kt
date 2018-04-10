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
package jetbrains.exodus.entitystore.replication

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.Protocol
import software.amazon.awssdk.http.SdkHttpFullRequest
import software.amazon.awssdk.http.SdkHttpMethod
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.http.SdkRequestContext
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.async.SdkHttpRequestProvider
import software.amazon.awssdk.http.async.SdkHttpResponseHandler
import software.amazon.awssdk.http.async.SimpleSubscriber
import software.amazon.awssdk.utils.BinaryUtils
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

internal val noBytes = ByteArray(size = 0)

internal val sdkRequestContext = SdkRequestContext.builder().build()

fun SdkAsyncHttpClient.postRequest(path: String, params: Map<String, String>, host: String, port: Int = 8062): ByteArray {
    val future = CompletableFuture<ByteArray>()

    prepareRequest(
            with(SdkHttpFullRequest.builder()) {
                host(host)
                port(port)
                protocol(Protocol.HTTP.name)
                method(SdkHttpMethod.POST)
                encodedPath(path)
                params.forEach {
                    rawQueryParameter(it.key, it.value)
                }
                header("Host", "$host:$port")
            }.build(),
            sdkRequestContext,
            EmptyRequestProvider,
            object : SdkHttpResponseHandler<Any> {
                private lateinit var baos: ByteArrayOutputStream

                override fun headersReceived(response: SdkHttpResponse) {
                    if (response.statusCode() != 200) {
                        future.completeExceptionally(IllegalStateException("unexpected status code ${response.statusCode()}"))
                    }
                }

                override fun complete(): Any? {
                    if (!::baos.isInitialized) {
                        future.completeExceptionally(IllegalStateException("no content"))
                    }
                    future.complete(baos.toByteArray())
                    return null
                }

                override fun onStream(publisher: Publisher<ByteBuffer>) {
                    baos = ByteArrayOutputStream()
                    publisher.subscribe(SimpleSubscriber { bb ->
                        try {
                            baos.write(BinaryUtils.copyBytesFrom(bb))
                        } catch (e: IOException) {
                            throw UncheckedIOException(e)
                        }
                    })
                }

                override fun exceptionOccurred(throwable: Throwable) {
                    future.completeExceptionally(throwable)
                }
            }
    ).run()
    return future.get()
}

internal object EmptyRequestProvider : SdkHttpRequestProvider {
    override fun contentLength(): Long = 0L

    override fun subscribe(s: Subscriber<in ByteBuffer>) {
        s.onSubscribe(object : Subscription {
            override fun request(n: Long) {
                s.onNext(ByteBuffer.wrap(noBytes))
                s.onComplete()
            }

            override fun cancel() {}
        })
    }
}
