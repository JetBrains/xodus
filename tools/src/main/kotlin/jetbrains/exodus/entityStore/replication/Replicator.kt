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
package jetbrains.exodus.entityStore.replication

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.newPersistentEntityStoreConfig
import jetbrains.exodus.entitystore.replication.S3Replicator
import jetbrains.exodus.env.Reflect
import software.amazon.awssdk.core.auth.AwsCredentials
import software.amazon.awssdk.core.auth.StaticCredentialsProvider
import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.io.File
import java.net.URI
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    if (args.size < 7) {
        printUsage()
        return
    }

    val region = Region.US_WEST_2
    val factory = NettySdkHttpClientFactory.builder().build()
    val httpClient = factory.createHttpClient()

    val bucket = args[0]
    val host = args[1]
    val port = Integer.parseInt(args[2])
    val accessKey = args[3]
    val secretKey = args[4]

    val s3 = S3AsyncClient.builder()
            .asyncHttpConfiguration(
                    ClientAsyncHttpConfiguration.builder().httpClient(httpClient).build()
            )
            .region(region)
            .endpointOverride(URI("http://$host:$port"))
            .advancedConfiguration(S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build()) // for minio
            .credentialsProvider(StaticCredentialsProvider.create(AwsCredentials.create(accessKey, secretKey)))
            .build()
    val replicator = S3Replicator(
            metaServer = host,
            httpClient = httpClient,
            s3 = s3,
            bucket = bucket
    )
    val environment = Reflect.openEnvironment(File(args[6]), true)
    println("Log tip: " + environment.log.tip.highAddress)
    val config = newPersistentEntityStoreConfig { storeReplicator = replicator }
    val store = PersistentEntityStoreImpl(config, environment, null, args[5])

    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        println("closing...")
        store.close()
        println("success")
    })
}

internal fun printUsage() {
    println("Usage: Replicator <bucket> <host> <port> <accessKey> <secretKey> <persistentStoreName> <path>")
}
