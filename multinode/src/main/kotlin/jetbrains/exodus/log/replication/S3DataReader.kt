/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.log.LogTip
import mu.KLogging
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.util.*
import kotlin.Comparator

class S3DataReader(override val s3: S3AsyncClient,
                   override val bucket: String,
                   override val requestOverrideConfig: AwsRequestOverrideConfiguration? = null,
                   val writer: S3DataWriter) : S3DataReaderOrWriter, DataReader {

    companion object : KLogging()

    override val logTip: LogTip? get() = writer.logTip

    @Suppress("UNCHECKED_CAST")
    override fun getBlocks(): Iterable<Block> {
        return logTip?.cachedBlocks ?: TreeSet<Block>(Comparator<Block> { o1, o2 ->
            o1.address.compareTo(o2.address)
        }).apply {
            addAll(fileBlocks)
            addAll(folderBlocks)
            logTip?.setCachedBlocks(this)
        }
    }

    override fun getBlocks(fromAddress: Long): Iterable<Block> = blocks.filter { it.address >= fromAddress }.toList()

    override fun getLocation(): String = "s3:$bucket"

    override fun close() = s3.close()

    internal fun getBlock(address: Long): Block {
        return blocks.firstOrNull { it.address == address }
                ?: throw ExodusException("No block found by address $address")
    }
}
