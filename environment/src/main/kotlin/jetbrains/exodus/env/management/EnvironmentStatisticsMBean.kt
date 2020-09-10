/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.env.management

const val STATISTICS_OBJECT_NAME_PREFIX = "jetbrains.exodus.env: type=EnvironmentStatistics"

interface EnvironmentStatisticsMBean {
    val bytesWritten: Long
    val bytesWrittenPerSecond: Double
    val bytesRead: Long
    val bytesReadPerSecond: Double
    val bytesMovedByGC: Long
    val bytesMovedByGCPerSecond: Double
    val logCacheHitRate: Float
    val numberOfTransactions: Long
    val numberOfTransactionsPerSecond: Double
    val numberOfReadonlyTransactions: Long
    val numberOfReadonlyTransactionsPerSecond: Double
    val activeTransactions: Int
    val numberOfFlushedTransactions: Long
    val numberOfFlushedTransactionsPerSecond: Double
    val diskUsage: Long
    val utilizationPercent: Int
    val storeGetCacheHitRate: Float
    val stuckTransactionCount: Int
}
