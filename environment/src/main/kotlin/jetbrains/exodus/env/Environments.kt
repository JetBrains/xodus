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
package jetbrains.exodus.env

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobHandler
import jetbrains.exodus.crypto.newCipherProvider
import jetbrains.exodus.io.AsyncFileDataWriter
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.LockingManager
import jetbrains.exodus.io.SharedOpenFilesCache
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.StartupMetadata
import jetbrains.exodus.tree.ExpiredLoggableCollection
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch

object Environments {

    @JvmStatic
    fun newInstance(dir: String, subDir: String, ec: EnvironmentConfig): Environment =
        prepare { EnvironmentImpl(newLogInstance(File(dir, subDir), ec), ec) }

    @JvmStatic
    fun newInstance(dir: String): Environment = newInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newInstance(log: Log, ec: EnvironmentConfig): Environment = prepare { EnvironmentImpl(log, ec) }

    @JvmStatic
    fun newInstance(dir: String, ec: EnvironmentConfig): Environment =
        prepare { EnvironmentImpl(newLogInstance(File(dir), ec), ec) }

    @JvmStatic
    fun newInstance(dir: File): Environment = newInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newInstance(dir: File, ec: EnvironmentConfig): Environment =
        prepare { EnvironmentImpl(newLogInstance(dir, ec), ec) }

    @JvmStatic
    fun newInstance(config: LogConfig): Environment = newInstance(config, EnvironmentConfig())

    @JvmStatic
    fun newInstance(config: LogConfig, ec: EnvironmentConfig): Environment =
        prepare { EnvironmentImpl(newLogInstance(config, ec), ec) }

    @JvmStatic
    fun <T : EnvironmentImpl> newInstance(envCreator: () -> T): T = prepare(envCreator)

    @Suppress("unused")
    @JvmStatic
    fun newContextualInstance(dir: String, subDir: String, ec: EnvironmentConfig): ContextualEnvironment =
        prepare { ContextualEnvironmentImpl(newLogInstance(File(dir, subDir), ec), ec) }

    @Suppress("unused")
    @JvmStatic
    fun newContextualInstance(dir: String): ContextualEnvironment = newContextualInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newContextualInstance(dir: String, ec: EnvironmentConfig): ContextualEnvironment =
        prepare { ContextualEnvironmentImpl(newLogInstance(File(dir), ec), ec) }

    @Suppress("unused")
    @JvmStatic
    fun newContextualInstance(dir: File): ContextualEnvironment = newContextualInstance(dir, EnvironmentConfig())

    @JvmStatic
    fun newContextualInstance(dir: File, ec: EnvironmentConfig): ContextualEnvironment =
        prepare { ContextualEnvironmentImpl(newLogInstance(dir, ec), ec) }

    @JvmStatic
    fun newContextualInstance(config: LogConfig, ec: EnvironmentConfig): ContextualEnvironment =
        prepare { ContextualEnvironmentImpl(newLogInstance(config, ec), ec) }

    @JvmStatic
    fun newLogInstance(dir: File, ec: EnvironmentConfig): Log = newLogInstance(LogConfig().setLocation(dir.path), ec)

    @JvmStatic
    fun newLogInstance(config: LogConfig, ec: EnvironmentConfig): Log {
        return newLogInstance(config.apply {
            val maxMemory = ec.memoryUsage
            if (maxMemory != null) {
                memoryUsage = maxMemory
            } else {
                memoryUsagePercentage = ec.memoryUsagePercentage
            }
            setReaderWriterProvider(ec.logDataReaderWriterProvider)

            fileSize = ec.logFileSize
            lockTimeout = ec.logLockTimeout
            cachePageSize = ec.logCachePageSize
            cacheOpenFilesCount = ec.logCacheOpenFilesCount
            isDurableWrite = ec.logDurableWrite
            isSharedCache = ec.isLogCacheShared
            isNonBlockingCache = ec.isLogCacheNonBlocking
            cacheUseSoftReferences = ec.logCacheUseSoftReferences
            cacheGenerationCount = ec.logCacheGenerationCount
            isCleanDirectoryExpected = ec.isLogCleanDirectoryExpected
            isClearInvalidLog = ec.isLogClearInvalid
            isWarmup = ec.logCacheWarmup
            syncPeriod = ec.logSyncPeriod
            isFullFileReadonly = ec.isLogFullFileReadonly
            cipherProvider = ec.cipherId?.let { cipherId -> newCipherProvider(cipherId) }
            cipherKey = ec.cipherKey
            cipherBasicIV = ec.cipherBasicIV
            isCheckPagesAtRuntime = ec.checkPagesAtRuntime

            setUseV1Format(ec.useVersion1Format)
        })
    }

    @JvmStatic
    fun newLogInstance(config: LogConfig): Log = Log(
        config.also
        { SharedOpenFilesCache.setSize(config.cacheOpenFilesCount) }, EnvironmentImpl.CURRENT_FORMAT_VERSION
    )

    private fun <T : EnvironmentImpl> prepare(envCreator: () -> T): T {
        var env = envCreator()
        val ec = env.environmentConfig
        val needsToBeMigrated = !env.log.formatWithHashCodeIsUsed

        if (ec.logDataReaderWriterProvider == DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER &&
            ec.envCompactOnOpen && env.log.numberOfFiles > 1 || needsToBeMigrated
        ) {
            if (needsToBeMigrated) {
                EnvironmentImpl.loggerInfo(
                    "Outdated binary format is used in environment ${env.log.location} " +
                            "migration of binary format will be performed."
                )
            }

            if (ec.envIsReadonly) {
                throw ExodusException("Can't compact readonly environment: ${env.log.location}")
            }

            val location = env.location
            File(location, "compactTemp${System.currentTimeMillis()}").let { tempDir ->
                if (!tempDir.mkdir()) {
                    EnvironmentImpl.loggerError("Failed to create temporary directory: $tempDir")
                    return@let
                }
                if (tempDir.freeSpace < env.diskUsage) {
                    EnvironmentImpl.loggerError("Not enough free disk space to compact the database: $location")
                    tempDir.delete()
                    return@let
                }
                env.copyTo(tempDir, false, null) { msg ->
                    EnvironmentImpl.loggerInfo(msg.toString())
                }
                env.close()

                LogUtil.listFiles(File(location)).forEach { file ->
                    if (!AsyncFileDataWriter.renameFile(file)) {
                        EnvironmentImpl.loggerError("Failed to rename file: $file")
                        return@let
                    }
                }

                val locationPath = Paths.get(location)

                val firstMetadataPath = locationPath.resolve(StartupMetadata.FIRST_FILE_NAME)

                if (Files.exists(firstMetadataPath)) {
                    val delFirstMetadataPath = locationPath.resolve(
                        StartupMetadata.FIRST_FILE_NAME +
                                AsyncFileDataWriter.DELETED_FILE_EXTENSION
                    )
                    Files.move(firstMetadataPath, delFirstMetadataPath)
                }

                val secondMetadataPath = locationPath.resolve(StartupMetadata.SECOND_FILE_NAME)

                if (Files.exists(secondMetadataPath)) {
                    val delSecondMetadataPath = locationPath.resolve(
                        StartupMetadata.SECOND_FILE_NAME +
                                AsyncFileDataWriter.DELETED_FILE_EXTENSION
                    )
                    Files.move(secondMetadataPath, delSecondMetadataPath)
                }

                LogUtil.listFiles(tempDir).forEach { file ->
                    if (!file.renameTo(File(location, file.name))) {
                        throw ExodusException("Failed to rename file: $file")
                    }
                }

                LogUtil.listMetadataFiles(tempDir).forEach { file ->
                    if (!file.renameTo(File(location, file.name))) {
                        throw ExodusException("Failed to rename file: $file")
                    }
                }

                Files.deleteIfExists(Paths.get(tempDir.toURI()).resolve(LockingManager.LOCK_FILE_NAME))

                env = envCreator()

                if (needsToBeMigrated) {
                    EnvironmentImpl.loggerInfo(
                        "Migration of binary format in environment ${env.log.location}" +
                                " has been completed. Please delete all files with extension " +
                                "*.del once you ensure database consistency."
                    )

                }

                env.flushAndSync()

                env.checkBlobs = true
                env.isCheckLuceneDirectory = true

                tempDir.delete()
            }
        }

        if (env.log.isClosedCorrectly) {
            if (env.log.formatWithHashCodeIsUsed) {
                env.gc.utilizationProfile.load()
                val rootAddress = env.metaTree.rootAddress()
                val rootLoggable = env.log.read(rootAddress)

                //once we close the log, the rest of the page is padded with null loggables
                //this free space should be taken into account during next open
                val paddedSpace = env.log.dataSpaceLeftInPage(rootAddress)
                val expiredLoggableCollection = ExpiredLoggableCollection.newInstance(env.log)

                expiredLoggableCollection.add(rootLoggable.end(), paddedSpace)
                env.gc.utilizationProfile.fetchExpiredLoggables(expiredLoggableCollection)
            }

            if (env.log.restoredFromBackup) {
                env.checkBlobs = true
                env.isCheckLuceneDirectory = true
            }
        } else {
            EnvironmentImpl.loggerInfo(
                "Because environment ${env.log.location} was closed incorrectly space utilization " +
                        "will be computed from scratch"
            )
            if (env.environmentConfig.isGcEnabled) {
                env.gc.suspend()
            }
            val job = env.gc.utilizationProfile.computeUtilizationFromScratch()
            if (job != null) {
                val latch = CountDownLatch(1)

                job.registerJobFinishedHandler(object : JobHandler() {
                    override fun handle(job: Job) {
                        latch.countDown()
                    }
                })

                if (!job.isCompleted) {
                    latch.await()
                }
            }
            if (env.environmentConfig.isGcEnabled) {
                env.gc.resume()
            }

            env.checkBlobs = true
            env.isCheckLuceneDirectory = true

            EnvironmentImpl.loggerInfo("Computation of space utilization for environment ${env.log.location} is completed")
        }

        val metaServer = ec.metaServer
        metaServer?.start(env)
        return env
    }
}