/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.entityStore.replication

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.newPersistentEntityStoreConfig
import jetbrains.exodus.entitystore.replication.S3Replicator
import jetbrains.exodus.env.Reflect
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Configuration
import java.io.File
import java.net.URI
import kotlin.concurrent.thread
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val region = Region.US_WEST_2
    val httpClient = NettyNioAsyncHttpClient.builder().build()

    var bucket: String? = null
    var host: String? = null
    var port = 9000
    var accessKey: String? = null
    var secretKey: String? = null
    var location: String? = null
    var persistentStoreName: String? = null

    var i = 0
    while (i < args.size) {
        val arg = args[i]
        if (arg.startsWith('-') && arg.length < 3) {
            when (arg.toLowerCase().substring(1)) {
                "b" -> bucket = args[++i]
                "h" -> host = args[++i]
                "p" -> port = Integer.parseInt(args[++i])
                "a" -> accessKey = args[++i]
                "s" -> secretKey = args[++i]
                "l" -> location = args[++i]
                "n" -> persistentStoreName = args[++i]
                else -> {
                    printUsage()
                    exitProcess(1)
                }
            }
        }
        i++
    }

    if (bucket == null || host == null || accessKey == null || secretKey == null || location == null || persistentStoreName == null) {
        printUsage()
        exitProcess(1)
    }

    val s3 = S3AsyncClient.builder()
            .httpClient(httpClient)
            .region(region)
            .endpointOverride(URI("http://$host:$port"))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()) // for minio
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .build()
    val replicator = S3Replicator(
            metaServer = host,
            httpClient = httpClient,
            s3 = s3,
            bucket = bucket
    )
    val environment = Reflect.openEnvironment(File(location), true)
    println("Log tip: " + environment.log.tip.highAddress)
    val config = newPersistentEntityStoreConfig { storeReplicator = replicator }
    val store = PersistentEntityStoreImpl(config, environment, null, persistentStoreName)

    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        println("closing...")
        store.close()
        println("success")
    })
}

internal fun printUsage() {
    println("Usage: Replicator -b <bucket> -h <host> -p <port> -a <accessKey> -s <secretKey> -n <persistentStoreName> -l <location>")
}
