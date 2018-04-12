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

import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.*
import io.netty.util.AsciiString
import jetbrains.exodus.entitystore.MetaServer
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.replication.ReplicationDelta
import java.util.concurrent.ConcurrentHashMap

class MetaServerImpl @JvmOverloads constructor(port: Int = 8062) : AutoCloseable, MetaServer {
    private val group = NioEventLoopGroup()
    private val channelHolder: ChannelFuture
    internal val environments: MutableMap<String, EnvironmentImpl> = ConcurrentHashMap()

    init {
        val bootstrap = ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().apply {
                            addLast("encoder", HttpResponseEncoder())
                            addLast("decoder", HttpRequestDecoder(4096, 8192, 8192, false))
                            addLast("handler", MetaServerHandler(this@MetaServerImpl))
                        }
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
        channelHolder = bootstrap.bind(port).sync()
    }

    override fun start(environment: Environment) {
        environments[environment.location] = environment as EnvironmentImpl
    }

    override fun stop(environment: Environment) {
        environments.remove(environment.location)
    }

    override fun close() {
        group.shutdownGracefully()
        channelHolder.channel().closeFuture().sync()
    }
}

class MetaServerHandler(val server: MetaServerImpl) : SimpleChannelInboundHandler<Any>() {
    companion object {
        private val applicationJson = AsciiString("application/json; charset=UTF-8")
        private val serverName = AsciiString("Xodus")
        private val contentTypeKey = AsciiString(HttpHeaderNames.CONTENT_TYPE)
        private val contentLengthKey = AsciiString(HttpHeaderNames.CONTENT_LENGTH)
        private val serverKey = AsciiString(HttpHeaderNames.SERVER)
        private val mapper: ObjectMapper = ObjectMapper()
        private val ok = OK()
    }

    private val gcTransactions: MutableMap<Long, GcTransaction> = ConcurrentHashMap()

    override fun channelRead0(ctx: ChannelHandlerContext, msg: Any) {
        if (msg is HttpRequest) {
            val decoder = QueryStringDecoder(msg.uri())
            val path = decoder.path()
            if (msg.method() != HttpMethod.POST) {
                respondEmpty(ctx, HttpResponseStatus.BAD_REQUEST)
                return
            }
            when {
                path.endsWith("/v1/delta/acquire") -> {
                    val from = decoder.parameters()["fromAddress"].toLongSafe(ctx)
                    val env = decoder.parameters()["location"].getString().let {
                        if (it == null) {
                            if (server.environments.size == 1) {
                                server.environments.entries.first().value
                            } else {
                                null
                            }
                        } else {
                            server.environments[it]
                        }
                    }
                    if (env == null) {
                        respondEmpty(ctx)
                        return
                    }
                    if (from >= 0) {
                        val gcTransaction = GcTransaction(env)
                        gcTransactions[gcTransaction.id] = gcTransaction
                        gcTransaction.start()
                        val metaTree = env.metaTree // TODO: fix possible race condition?
                        val tip = metaTree.logTip
                        val result = ReplicationDelta(
                                startAddress = from,
                                highAddress = tip.highAddress,
                                fileLengthBound = env.log.fileLengthBound,
                                files = tip.getFilesFrom(from).asSequence().toList().toLongArray(),
                                metaTreeAddress = metaTree.treeAddress(),
                                rootAddress = metaTree.rootAddress(),
                                encrypted = env.cipherProvider != null,
                                id = gcTransaction.id
                        )
                        respond(msg, ctx, result)
                    }
                    return
                }
                path.endsWith("/v1/delta/release") -> {
                    val id = decoder.parameters()["id"].toLongSafe(ctx)
                    if (id >= 0) {
                        gcTransactions[id]?.let {
                            try {
                                it.stop()
                            } finally {
                                gcTransactions.remove(id)
                            }
                            respond(msg, ctx, ok)
                        } ?: respondEmpty(ctx, HttpResponseStatus.NOT_FOUND)
                    }
                    return
                }
            }
            respondEmpty(ctx)
        }
    }

    private fun List<String>?.toLongSafe(ctx: ChannelHandlerContext): Long {
        if (this != null && isNotEmpty()) {
            try {
                return this[0].toLong()
            } catch (e: NumberFormatException) {
            }
        }
        respondEmpty(ctx, HttpResponseStatus.BAD_REQUEST)
        return -1
    }

    private fun List<String>?.getString(): String? {
        if (this != null && isNotEmpty()) {
            return this[0]
        }
        return null
    }

    private fun respond(request: HttpRequest, ctx: ChannelHandlerContext, payload: Any) {
        val bytes = mapper.writeValueAsBytes(payload)
        val keepAlive = HttpUtil.isKeepAlive(request)
        val response = DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(bytes),
                false
        )
        response.headers().apply {
            set(contentTypeKey, applicationJson)
            set(serverKey, serverName)
            set(contentLengthKey, bytes.size.toString())
        }
        if (!keepAlive) {
            ctx.write(response).addListener(ChannelFutureListener.CLOSE)
        } else {
            ctx.write(response, ctx.voidPromise())
        }
    }

    private fun respondEmpty(ctx: ChannelHandlerContext, status: HttpResponseStatus = HttpResponseStatus.NOT_FOUND) {
        DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.EMPTY_BUFFER,
                false
        ).let {
            ctx.write(it).addListener(ChannelFutureListener.CLOSE)
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
    }

    override fun channelReadComplete(ctx: ChannelHandlerContext) {
        ctx.flush()
    }

    data class OK(val ok: Boolean = true)
}
