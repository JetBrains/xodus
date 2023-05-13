/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.env.management

const val STATISTICS_OBJECT_NAME_PREFIX = "jetbrains.exodus.env: type=EnvironmentStatistics"

interface EnvironmentStatisticsMBean {
    fun getBytesWritten(): Long
    fun getBytesWrittenPerSecond(): Double
    fun getBytesRead(): Long
    fun getBytesReadPerSecond(): Double
    fun getBytesMovedByGC(): Long
    fun getBytesMovedByGCPerSecond(): Double
    fun getLogCacheHitRate(): Float
    fun getNumberOfTransactions(): Long
    fun getNumberOfTransactionsPerSecond(): Double
    fun getNumberOfReadonlyTransactions(): Long
    fun getNumberOfReadonlyTransactionsPerSecond(): Double
    fun getNumberOfGCTransactions(): Long
    fun getNumberOfGCTransactionsPerSecond(): Double
    fun getActiveTransactions(): Int
    fun getNumberOfFlushedTransactions(): Long
    fun getNumberOfFlushedTransactionsPerSecond(): Double
    fun getTransactionsDuration(): Long
    fun getReadonlyTransactionsDuration(): Long
    fun getGcTransactionsDuration(): Long
    fun getDiskUsage(): Long
    fun getUtilizationPercent(): Int
    fun getStoreGetCacheHitRate(): Float
    fun getStuckTransactionCount(): Int
}
