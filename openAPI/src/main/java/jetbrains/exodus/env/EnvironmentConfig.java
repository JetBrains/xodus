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
package jetbrains.exodus.env;

import jetbrains.exodus.*;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.crypto.KryptKt;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Specifies settings of {@linkplain Environment}. Default settings are specified by {@linkplain #DEFAULT} which
 * is immutable. Any newly created {@code EnvironmentConfig} has the same settings as {@linkplain #DEFAULT}.
 *
 * <p>As a rule, the {@code EnvironmentConfig} instance is created along with the {@linkplain Environment} one.
 * E.g., creation of {@linkplain Environment} with some tuned garbage collector settings can look as follows:
 * <pre>
 *     final EnvironmentConfig config = new EnvironmentConfig();
 *     final Environment env = Environments.newInstance(""dbDirectory", config.setGcFileMinAge(3).setGcRunPeriod(60000));
 * </pre>
 *
 * Some setting are mutable at runtime and some are immutable. Immutable at runtime settings can be changed, but they
 * won't take effect on the {@linkplain Environment} instance. Those settings are applicable only during
 * {@linkplain Environment} instance creation.
 *
 * <p>Some settings can be changed only before the database is physically created on storage device. E.g.,
 * {@linkplain #LOG_FILE_SIZE} defines the size of a single {@code Log} file (.xd file) which cannot be changed for
 * existing databases. It makes sense to choose values of such settings at design time and hardcode them.
 *
 * <p>You can define custom processing of changed settings values by
 * {@linkplain #addChangedSettingsListener(ConfigSettingChangeListener)}. Override
 * {@linkplain ConfigSettingChangeListener#beforeSettingChanged(String, Object, Map)} to pre-process mutations of
 * settings and {@linkplain ConfigSettingChangeListener#afterSettingChanged(String, Object, Map)} to post-process them.
 *
 * @see Environment
 * @see Environment#getEnvironmentConfig()
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class EnvironmentConfig extends AbstractConfig {

    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig(ConfigurationStrategy.IGNORE);

    /**
     * Defines absolute value of memory in bytes that can be used by the LogCache. By default, is not set.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE_PERCENTAGE
     */
    public static final String MEMORY_USAGE = "exodus.memoryUsage";

    /**
     * Defines percent of max memory (specified by the "-Xmx" java parameter) that can be used by the LogCache.
     * Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is {@code 50}.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE
     */
    public static final String MEMORY_USAGE_PERCENTAGE = "exodus.memoryUsagePercentage";

    /**
     * Defines id of {@linkplain StreamCipherProvider} which will be used to encrypt the database. Default value is
     * {@code null}, which means that the database won't be encrypted. The setting cannot be changed for existing databases.
     * Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @see #CIPHER_KEY
     * @see #CIPHER_BASIC_IV
     * @see StreamCipher
     * @see StreamCipherProvider
     */
    public static final String CIPHER_ID = "exodus.cipherId";

    /**
     * Defines the key which will be used to encrypt the database. The key is expected to be a hex string representing
     * a byte array which is passed to {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if
     * {@linkplain #CIPHER_ID} is not {@code null}. The setting cannot be changed for existing databases.
     * Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @see #CIPHER_ID
     * @see #CIPHER_BASIC_IV
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    public static final String CIPHER_KEY = "exodus.cipherKey";

    /**
     * Defines basic IV (initialization vector) which will be used to encrypt the database. Basic IV is expected to be
     * random (pseudo-random) and unique long value. Basic IV is used to calculate relative IVs which are passed to
     * {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if {@linkplain #CIPHER_ID} is not {@code null}.
     * The setting cannot be changed for existing databases.
     * Default value is {@code 0L}.
     * <p>Mutable at runtime: no
     *
     * @see #CIPHER_ID
     * @see #CIPHER_KEY
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    public static final String CIPHER_BASIC_IV = "exodus.cipherBasicIV";

    /**
     * If is set to {@code true} forces file system's fsync call after each committed or flushed transaction. By default,
     * is switched off since it creates great performance overhead and can be controlled manually.
     * <p>Mutable at runtime: yes
     */
    public static final String LOG_DURABLE_WRITE = "exodus.log.durableWrite";

    /**
     * Defines the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting cannot be changed
     * for existing databases. Default value is {@code 8192L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_FILE_SIZE = "exodus.log.fileSize";

    /**
     * Defines the number of milliseconds the Log constructor waits for the lock file. Default value is {@code 0L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_LOCK_TIMEOUT = "exodus.log.lockTimeout";

    /**
     * Defines the debug identifier to be written to the lock file alongside with other debug information.
     * Default value is {@code ManagementFactory.getRuntimeMXBean().getName()} which has a form of {@code pid@hostname}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_LOCK_ID = "exodus.log.lockID";

    /**
     * Defines the size in bytes of a single page (byte array) in the LogCache. This number of bytes is read from
     * {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at a time.
     *
     * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s should be configured
     * to use single LogCache page size.
     *
     * <p>Default value is {@code 64 * 1024}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_PAGE_SIZE = "exodus.log.cache.pageSize";

    /**
     * Defines the maximum number of open files that LogCache maintains in order to reduce system calls to open and
     * close files. The more open files is allowed the less system calls are performed in addition to reading or
     * mapping files. This value can notably affect performance of database warm-up. Default value is {@code 500}.
     * Open files cache is shared amongst all open {@linkplain Environment environments}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_OPEN_FILES = "exodus.log.cache.openFilesCount";

    /**
     * If is set to {@code true} any immutable file can be mapped in memory provided there is enough physical memory.
     * On cache miss, LogCache at first tries to check if there is corresponding mapped byte buffer and copies
     * cache page from the buffer, otherwise reads the page from {@linkplain java.io.RandomAccessFile}. If is set to
     * {@code false} then LogCache always reads {@linkplain java.io.RandomAccessFile} on cache miss.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see #LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD
     */
    public static final String LOG_CACHE_USE_NIO = "exodus.log.cache.useNIO";

    /**
     * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} defines the minimum size in bytes of free physical memory
     * maintained by the cache memory-mapped files on the host where the JVM runs. On cache miss, LogCache checks if
     * corresponding file is mapped in memory, and if it is not and if free physical memory amount is greater than
     * threshold specified by this setting, tries to map the file in memory. Default value is {@code 1_000_000_000L}
     * bytes, i.e. ~1GB.
     * <p>Mutable at runtime: no
     *
     * @see #LOG_CACHE_USE_NIO
     */
    public static final String LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD = "exodus.log.cache.freePhysicalMemoryThreshold";

    /**
     * If is set to {@code true} the LogCache is shared. Shared cache caches raw binary
     * data (contents of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
     * By default, the LogCache is shared.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_SHARED = "exodus.log.cache.shared";

    /**
     * If is set to {@code true} the LogCache uses lock-free data structures. Default value is {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_NON_BLOCKING = "exodus.log.cache.nonBlocking";

    /**
     * If is set to {@code true} then the Log constructor fails if the database directory is not clean. Can be useful
     * if an applications expects that the database should always be newly created. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CLEAN_DIRECTORY_EXPECTED = "exodus.log.cleanDirectoryExpected";

    /**
     * If is set to {@code true} then the Log constructor implicitly clears the database if it occurred to be invalid
     * when opened. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CLEAR_INVALID = "exodus.log.clearInvalid";

    /**
     * Sets the period in milliseconds to force file system's fsync call that often if {@linkplain #LOG_DURABLE_WRITE}
     * is switched off. Default value is {@code 10000L}.
     * <p>Mutable at runtime: yes
     *
     * @see #LOG_DURABLE_WRITE
     */
    public static final String LOG_SYNC_PERIOD = "exodus.log.syncPeriod";

    /**
     * If is set to {@code true} then each complete and immutable {@code Log} file (.xd file) is marked with read-only
     * attribute. Default value is {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_FULL_FILE_READ_ONLY = "exodus.log.fullFileReadonly";

    /**
     * If is set to {@code true} then the {@linkplain Environment} instance is read-only. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String ENV_IS_READONLY = "exodus.env.isReadonly";

    /**
     * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
     * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a {@linkplain Store},
     * but returns an empty immutable instance instead. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @see #ENV_IS_READONLY
     */
    public static final String ENV_READONLY_EMPTY_STORES = "exodus.env.readonly.emptyStores";

    /**
     * Defines the size of the "store-get" cache. The "store-get" cache can increase performance of
     * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0} what means that
     * the cache is inactive. If the setting is mutated at runtime the cache is invalidated.
     * <p>Mutable at runtime: yes
     */
    public static final String ENV_STOREGET_CACHE_SIZE = "exodus.env.storeGetCacheSize";

    /**
     * If is set to {@code true} then {@linkplain Environment#close()} doest't check if there are unfinished
     * transactions. Otherwise it checks and throws {@linkplain ExodusException} if there are.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @see Environment#close()
     */
    public static final String ENV_CLOSE_FORCEDLY = "exodus.env.closeForcedly";

    /**
     * Defines the number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2000L}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_MAX_COUNT
     * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
     */
    public static final String ENV_TXN_REPLAY_TIMEOUT = "exodus.env.txn.replayTimeout";

    /**
     * Defines the number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_TIMEOUT
     * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
     */
    public static final String ENV_TXN_REPLAY_MAX_COUNT = "exodus.env.txn.replayMaxCount";

    /**
     * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself after
     * {@linkplain Transaction#flush()}. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @see Transaction
     * @see #ENV_TXN_REPLAY_TIMEOUT
     * @see #ENV_TXN_REPLAY_MAX_COUNT
     */
    public static final String ENV_TXN_DOWNGRADE_AFTER_FLUSH = "exodus.env.txn.downgradeAfterFlush";

    /**
     * Defines the number of {@linkplain Transaction transactions} that can be started in parallel. By default it is
     * unlimited.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see #ENV_MAX_PARALLEL_READONLY_TXNS
     */
    public static final String ENV_MAX_PARALLEL_TXNS = "exodus.env.maxParallelTxns";

    /**
     * Defines the number of read-only {@linkplain Transaction transactions} that can be started in parallel. By
     * default it is unlimited.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see Transaction#isReadonly()
     * @see #ENV_MAX_PARALLEL_TXNS
     */
    public static final String ENV_MAX_PARALLEL_READONLY_TXNS = "exodus.env.maxParallelReadonlyTxns";

    /**
     * Defines {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this timeout then
     * it is reported in logs as stuck along with stack trace which it was created with. Default value is {@code 0}
     * which means that no timeout for a {@linkplain Transaction} is defined. In that case, no monitor of stuck
     * transactions is started. Otherwise it is started for each {@linkplain Environment}, though consuming only a
     * single {@linkplain Thread} amongst all environments created within a single class loader.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see #ENV_MONITOR_TXNS_CHECK_FREQ
     */
    public static final String ENV_MONITOR_TXNS_TIMEOUT = "exodus.env.monitorTxns.timeout";

    /**
     * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts and checks
     * {@linkplain Environment}'s transactions with this frequency (period) specified in milliseconds.
     * Default value is {@code 60000}, one minute.
     * <p>Mutable at runtime: no
     *
     * @see Transaction
     * @see #ENV_MONITOR_TXNS_TIMEOUT
     */
    public static final String ENV_MONITOR_TXNS_CHECK_FREQ = "exodus.env.monitorTxns.checkFreq";

    /**
     * If is set to {@code true} then the {@linkplain Environment} gathers statistics. If
     * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX managed bean.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see Environment#getStatistics()
     * @see #MANAGEMENT_ENABLED
     */
    public static final String ENV_GATHER_STATISTICS = "exodus.env.gatherStatistics";

    /**
     * Defines the maximum size of page of B+Tree. Default value is {@code 128}.
     * <p>Mutable at runtime: no
     */
    public static final String TREE_MAX_PAGE_SIZE = "exodus.tree.maxPageSize";

    /**
     * As of 1.0.5, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     */
    @Deprecated
    public static final String TREE_NODES_CACHE_SIZE = "exodus.tree.nodesCacheSize";

    /**
     * If is set to {@code true} then the database garbage collector is enabled. Default value is {@code true}.
     * Switching GC off makes sense only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_ENABLED = "exodus.gc.enabled";

    /**
     * Defines the number of milliseconds which the database garbage collector is postponed for after the
     * {@linkplain Environment} is created. Default value is {@code 10000}.
     * <p>Mutable at runtime: no
     */
    public static final String GC_START_IN = "exodus.gc.startIn";

    /**
     * Defines percent of minimum database utilization. Default value is {@code 50}. That means that 50 percent
     * of free space in raw data in {@code Log} files (.xd files) is allowed. If database utilization is less than
     * defined (free space percent is more than {@code 50}), the database garbage collector is triggered.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_MIN_UTILIZATION = "exodus.gc.minUtilization";

    /**
     * If is set to {@code true} the database garbage collector renames files rather than deletes them. Default
     * value is {@code false}. It makes sense to change this setting only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_RENAME_FILES = "exodus.gc.renameFiles";

    /**
     * As of 1.0.2, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     */
    @Deprecated
    public static final String GC_USE_EXPIRATION_CHECKER = "exodus.gc.useExpirationChecker";

    /**
     * Defines the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the database garbage
     * collector. The age of the last (the newest, the rightmost) {@code Log} file is {@code 0}, the age of previous
     * file is {@code 1}, etc. Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_MIN_FILE_AGE = "exodus.gc.fileMinAge";

    /**
     * Defines the number of new {@code Log} files (.xd files) that must be created to trigger if necessary (if database
     * utilization is not sufficient) the next background cleaning cycle (single run of the database garbage collector)
     * after the previous cycle finished. Default value is {@code 3}, i.e. GC can start after each 3 newly created
     * {@code Log} files.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_FILES_INTERVAL = "exodus.gc.filesInterval";

    /**
     * Defines the number of milliseconds after which background cleaning cycle (single run of the database garbage
     * collector) can be repeated if the previous one didn't reach required utilization. Default value is
     * {@code 30000}.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_RUN_PERIOD = "exodus.gc.runPeriod";

    /**
     * If is set to {@code true} then database utilization will be computed from scratch before the first cleaning
     * cycle (single run of the database garbage collector) is triggered, i.e. shortly after the database is open.
     * In addition, can be used to compute utilization information at runtime by just modifying the setting value.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_UTILIZATION_FROM_SCRATCH = "exodus.gc.utilization.fromScratch";

    /**
     * If is not empty, defines full path to the file with stored utilization. Is used on creation of an
     * {@linkplain Environment} to update {@code .xd} files' utilization before the first cleaning
     * cycle (single run of the database garbage collector) is triggered. In addition, can be used to reload utilization
     * information at runtime by just modifying the setting value. Format of the stored utilization is expected
     * to be the same as created by the {@code "-d"} option of the {@code Reflect} tool.
     * Default value is empty string.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_UTILIZATION_FROM_FILE = "exodus.gc.utilization.fromFile";

    /**
     * If is set to {@code true} the database garbage collector tries to acquire exclusive {@linkplain Transaction}
     * for its purposes. In that case, GC transaction never re-plays. In order to not block background cleaner thread
     * forever, acquisition of exclusive GC transaction is performed with a timeout controlled by the
     * {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT} setting. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @see #GC_TRANSACTION_ACQUIRE_TIMEOUT
     * @see #GC_TRANSACTION_TIMEOUT
     * @see Transaction#isExclusive()
     */
    public static final String GC_USE_EXCLUSIVE_TRANSACTION = "exodus.gc.useExclusiveTransaction";

    /**
     * Defines timeout in milliseconds which is used by the database garbage collector to acquire exclusive
     * {@linkplain Transaction} for its purposes if {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @see #GC_USE_EXCLUSIVE_TRANSACTION
     * @see #GC_TRANSACTION_TIMEOUT
     * @see Transaction#isExclusive()
     */
    public static final String GC_TRANSACTION_ACQUIRE_TIMEOUT = "exodus.gc.transactionAcquireTimeout";

    /**
     * Defines timeout in milliseconds which is used by the database garbage collector to reclaim non-expired data
     * in several files inside single GC {@linkplain Transaction} acquired exclusively.
     * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @see #GC_USE_EXCLUSIVE_TRANSACTION
     * @see #GC_TRANSACTION_ACQUIRE_TIMEOUT
     * @see Transaction#isExclusive()
     */
    public static final String GC_TRANSACTION_TIMEOUT = "exodus.gc.transactionTimeout";

    /**
     * Defines the number of milliseconds which deletion of any successfully cleaned {@code Log} file (.xd file)
     * is postponed for. Default value is {@code 5000}.
     * <p>Mutable at runtime: yes
     */
    public static final String GC_FILES_DELETION_DELAY = "exodus.gc.filesDeletionDelay";

    /**
     * If is set to {@code true} then the {@linkplain Environment} exposes two JMX managed beans. One for
     * {@linkplain Environment#getStatistics() environment statistics} and second for controlling the
     * {@code EnvironmentConfig} settings. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see Environment#getStatistics()
     * @see Environment#getEnvironmentConfig()
     */
    public static final String MANAGEMENT_ENABLED = "exodus.managementEnabled";

    /**
     * If is set to {@code true} then exposed JMX managed beans cannot have operations.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @see #MANAGEMENT_ENABLED
     */
    public static final String MANAGEMENT_OPERATIONS_RESTRICTED = "exodus.management.operationsRestricted";

    public EnvironmentConfig() {
        this(ConfigurationStrategy.SYSTEM_PROPERTY);
    }

    public EnvironmentConfig(@NotNull final ConfigurationStrategy strategy) {
        //noinspection unchecked
        super(new Pair[]{
            new Pair(MEMORY_USAGE_PERCENTAGE, 50),
            new Pair(CIPHER_ID, null),
            new Pair(CIPHER_KEY, null),
            new Pair(CIPHER_BASIC_IV, 0L),
            new Pair(LOG_DURABLE_WRITE, false),
            new Pair(LOG_FILE_SIZE, 8192L),
            new Pair(LOG_LOCK_TIMEOUT, 0L),
            new Pair(LOG_LOCK_ID, null),
            new Pair(LOG_CACHE_PAGE_SIZE, 64 * 1024),
            new Pair(LOG_CACHE_OPEN_FILES, 500),
            new Pair(LOG_CACHE_USE_NIO, true),
            new Pair(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, 1_000_000_000L), // ~1GB
            new Pair(LOG_CACHE_SHARED, true),
            new Pair(LOG_CACHE_NON_BLOCKING, true),
            new Pair(LOG_CLEAN_DIRECTORY_EXPECTED, false),
            new Pair(LOG_CLEAR_INVALID, false),
            new Pair(LOG_SYNC_PERIOD, 10000L),
            new Pair(LOG_FULL_FILE_READ_ONLY, true),
            new Pair(ENV_IS_READONLY, false),
            new Pair(ENV_READONLY_EMPTY_STORES, false),
            new Pair(ENV_STOREGET_CACHE_SIZE, 0),
            new Pair(ENV_CLOSE_FORCEDLY, false),
            new Pair(ENV_TXN_REPLAY_TIMEOUT, 2000L),
            new Pair(ENV_TXN_REPLAY_MAX_COUNT, 2),
            new Pair(ENV_TXN_DOWNGRADE_AFTER_FLUSH, true),
            new Pair(ENV_MAX_PARALLEL_TXNS, Integer.MAX_VALUE),
            new Pair(ENV_MAX_PARALLEL_READONLY_TXNS, Integer.MAX_VALUE),
            new Pair(ENV_MONITOR_TXNS_TIMEOUT, 0),
            new Pair(ENV_MONITOR_TXNS_CHECK_FREQ, 60000),
            new Pair(ENV_GATHER_STATISTICS, true),
            new Pair(TREE_MAX_PAGE_SIZE, 128),
            new Pair(GC_ENABLED, true),
            new Pair(GC_START_IN, 10000),
            new Pair(GC_MIN_UTILIZATION, 50),
            new Pair(GC_RENAME_FILES, false),
            new Pair(GC_MIN_FILE_AGE, 2),
            new Pair(GC_FILES_INTERVAL, 3),
            new Pair(GC_RUN_PERIOD, 30000),
            new Pair(GC_UTILIZATION_FROM_SCRATCH, false),
            new Pair(GC_UTILIZATION_FROM_FILE, ""),
            new Pair(GC_FILES_DELETION_DELAY, 5000),
            new Pair(GC_USE_EXCLUSIVE_TRANSACTION, true),
            new Pair(GC_TRANSACTION_ACQUIRE_TIMEOUT, 1000),
            new Pair(GC_TRANSACTION_TIMEOUT, 1000),
            new Pair(MANAGEMENT_ENABLED, true),
            new Pair(MANAGEMENT_OPERATIONS_RESTRICTED, true)
        }, strategy);
    }

    /**
     * Sets the value of the setting with the specified key.
     *
     * @param key   name of the setting
     * @param value the setting value
     * @return this {@code EnvironmentConfig} instance
     */
    @Override
    public EnvironmentConfig setSetting(@NotNull final String key, @NotNull final Object value) {
        return (EnvironmentConfig) super.setSetting(key, value);
    }

    /**
     * Returns absolute value of memory in bytes that can be used by the LogCache if it is set.
     * By default, is not set.
     * <p>Mutable at runtime: no
     *
     * @return absolute value of memory in bytes that can be used by the LogCache if it is set or {@code null}
     * @see #getMemoryUsagePercentage()
     */
    public Long /* NB! do not change to long */ getMemoryUsage() {
        return (Long) getSetting(MEMORY_USAGE);
    }

    /**
     * Sets absolute value of memory in bytes that can be used by the LogCache. Overrides memory percent to
     * use by the LogCache set by {@linkplain #setMemoryUsagePercentage(int)}.
     * <p>Mutable at runtime: no
     *
     * @param maxMemory number of bytes that can be used by the LogCache
     * @return absolute this {@code EnvironmentConfig} instance
     * @see #setMemoryUsagePercentage(int)
     */
    public EnvironmentConfig setMemoryUsage(final long maxMemory) {
        return setSetting(MEMORY_USAGE, maxMemory);
    }

    /**
     * Returns percent of max memory (specified by the "-Xmx" java parameter) that can be used by the LogCache.
     * Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is {@code 50}.
     * <p>Mutable at runtime: no
     *
     * @return percent of max memory that can be used by the LogCache
     * @see #getMemoryUsage()
     */
    public int getMemoryUsagePercentage() {
        return (Integer) getSetting(MEMORY_USAGE_PERCENTAGE);
    }

    /**
     * Sets percent of max memory (specified by the "-Xmx" java parameter) that can be used by the LogCache.
     * Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is {@code 50}.
     * <p>Mutable at runtime: no
     *
     * @param memoryUsagePercentage percent of max memory that can be used by the LogCache
     * @return this {@code EnvironmentConfig} instance
     * @see #setMemoryUsage(long)
     */
    public EnvironmentConfig setMemoryUsagePercentage(final int memoryUsagePercentage) {
        return setSetting(MEMORY_USAGE_PERCENTAGE, memoryUsagePercentage);
    }

    /**
     * Returns id of {@linkplain StreamCipherProvider} which will be used to encrypt the database. Default value is
     * {@code null}, which means that the database won't be encrypted. The setting cannot be changed for existing databases.
     * Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @return id of {@linkplain StreamCipherProvider} which will be used to encrypt the database
     * @see #getCipherKey()
     * @see #getCipherBasicIV()
     * @see StreamCipher
     * @see StreamCipherProvider
     */
    @Nullable
    public String getCipherId() {
        return (String) getSetting(CIPHER_ID);
    }

    /**
     * Sets id of {@linkplain StreamCipherProvider} which will be used to encrypt the database. Default value is
     * {@code null}, which means that the database won't be encrypted. The setting cannot be changed for existing databases.
     * Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @param id id of {@linkplain StreamCipherProvider}
     * @return this {@code EnvironmentConfig} instance
     * @see #setCipherKey(String)
     * @see #setCipherBasicIV(long)
     * @see StreamCipher
     * @see StreamCipherProvider
     */
    public EnvironmentConfig setCipherId(final String id) {
        return setSetting(CIPHER_ID, id);
    }

    /**
     * Returns the key which will be used to encrypt the database or {@code null} for no encryption. Is applicable
     * only if* {@linkplain #getCipherId()} returns not {@code null}. Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @return the key which will be used to encrypt the database or {@code null} for no encryption
     * @see #getCipherId()
     * @see #getCipherBasicIV()
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    @Nullable
    public byte[] getCipherKey() {
        Object cipherKey = getSetting(CIPHER_KEY);
        if (cipherKey instanceof String) {
            cipherKey = KryptKt.toBinaryKey((String) cipherKey);
            setSetting(CIPHER_KEY, cipherKey);
        }
        return (byte[]) cipherKey;
    }

    /**
     * Sets the key which will be used to encrypt the database. The key is expected to be a hex string representing
     * a byte array which is passed to {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if
     * {@linkplain #getCipherId()} returns not {@code null}. The setting cannot be changed for existing databases.
     * Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @param cipherKey hex string representing cipher key
     * @return this {@code EnvironmentConfig} instance
     * @see #setCipherId(String)
     * @see #setCipherBasicIV(long)
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    public EnvironmentConfig setCipherKey(final String cipherKey) {
        if (cipherKey == null) {
            return (EnvironmentConfig) removeSetting(CIPHER_KEY);
        }
        return setSetting(CIPHER_KEY, KryptKt.toBinaryKey(cipherKey));
    }

    /**
     * Returns basic IV (initialization vector) which will be used to encrypt the database. Basic IV is expected to be
     * random (pseudo-random) and unique long value. Basic IV is used to calculate relative IVs which are passed to
     * {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if {@linkplain #CIPHER_ID} is not {@code null}.
     * The setting cannot be changed for existing databases.
     * Default value is {@code 0L}.
     * <p>Mutable at runtime: no
     *
     * @return basic IV (initialization vector) which will be used to encrypt the database
     * @see #getCipherId()
     * @see #getCipherKey()
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    public long getCipherBasicIV() {
        return (long) getSetting(CIPHER_BASIC_IV);
    }

    /**
     * Sets basic IV (initialization vector) which will be used to encrypt the database. Basic IV is expected to be
     * random (pseudo-random) and unique long value. Basic IV is used to calculate relative IVs which are passed to
     * {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if {@linkplain #CIPHER_ID} is not {@code null}.
     * The setting cannot be changed for existing databases.
     * Default value is {@code 0L}.
     * <p>Mutable at runtime: no
     *
     * @param basicIV basic IV (initialization vector) which will be used to encrypt the database
     * @return this {@code EnvironmentConfig} instance
     * @see #setCipherId(String)
     * @see #setCipherKey(String)
     * @see StreamCipher
     * @see StreamCipher#init(byte[], long)
     * @see StreamCipherProvider
     */
    public EnvironmentConfig setCipherBasicIV(final long basicIV) {
        return setSetting(CIPHER_BASIC_IV, basicIV);
    }

    /**
     * Returns {@code true} if file system's fsync call after should be  executed after each committed or flushed
     * transaction. By default, is switched off since it creates significant performance overhead and can be
     * controlled manually.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if {@linkplain Transaction transactions} are durable
     * @see #getLogSyncPeriod()
     */
    public boolean getLogDurableWrite() {
        return (Boolean) getSetting(LOG_DURABLE_WRITE);
    }

    /**
     * Sets flag whether {@linkplain Transaction transactions} should force fsync after each commit or flush.
     * By default, is switched off since it creates significant performance overhead and can be controlled manually.
     * <p>Mutable at runtime: yes
     *
     * @param durableWrite {@code true} if {@linkplain Transaction transactions} should be durable
     * @return this {@code EnvironmentConfig} instance
     * @see #setLogSyncPeriod(long)
     */
    public EnvironmentConfig setLogDurableWrite(final boolean durableWrite) {
        return setSetting(LOG_DURABLE_WRITE, durableWrite);
    }

    /**
     * Returns the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting cannot be changed
     * for existing databases. Default value is {@code 8192L}.
     * <p>Mutable at runtime: no
     *
     * @return maximum size in kilobytes of a single .xd file
     */
    public long getLogFileSize() {
        return (Long) getSetting(LOG_FILE_SIZE);
    }

    /**
     * Sets the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting cannot be changed
     * for existing databases. Default value is {@code 8192L}.
     * <p>Mutable at runtime: no
     *
     * @param kilobytes maximum size in kilobytes of a single .xd file
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogFileSize(final long kilobytes) {
        return setSetting(LOG_FILE_SIZE, kilobytes);
    }

    /**
     * Returns the number of milliseconds the {@code Log} constructor waits for the lock file.
     * Default value is {@code 0L}, i.e. it doesn't wait and fails immediately if the lock is acquired.
     * <p>Mutable at runtime: no
     *
     * @return number of milliseconds the {@code Log} constructor waits for the lock file
     */
    public long getLogLockTimeout() {
        return (Long) getSetting(LOG_LOCK_TIMEOUT);
    }

    /**
     * Sets the debug identifier to be written to the lock file alongside with other debug information.
     * <p>Mutable at runtime: no
     *
     * @param id the debug identifier to be written to the lock file alongside with other debug information
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogLockId(final String id) {
        return setSetting(LOG_LOCK_ID, id);
    }

    /**
     * Sets the number of milliseconds the {@code Log} constructor waits for the lock file.
     * <p>Mutable at runtime: no
     *
     * @param millis number of milliseconds the {@code Log} constructor should wait for the lock file
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogLockTimeout(final long millis) {
        return setSetting(LOG_LOCK_TIMEOUT, millis);
    }

    /**
     * Sets the debug identifier to be written to the lock file alongside with other debug information.
     * Default value is {@code ManagementFactory.getRuntimeMXBean().getName()} which has a form of {@code pid@hostname}.
     * <p>Mutable at runtime: no
     *
     * @return the debug identifier to be written to the lock file alongside with other debug information
     * or null if the default value is used
     */
    public String getLogLockId() {
        return (String) getSetting(LOG_LOCK_ID);
    }

    /**
     * Returns the size in bytes of a single page (byte array) in the LogCache. This number of bytes is read from
     * {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at a time.
     *
     * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s should be configured
     * to use single LogCache page size.
     *
     * <p>Default value is {@code 64 * 1024}.
     * <p>Mutable at runtime: no
     *
     * @return size in bytes of a single page (byte array) in the LogCache
     */
    public int getLogCachePageSize() {
        return (Integer) getSetting(LOG_CACHE_PAGE_SIZE);
    }

    /**
     * Sets the size in bytes of a single page (byte array) in the LogCache. This number of bytes is read from
     * {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at a time.
     *
     * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s should be configured
     * to use single LogCache page size.
     *
     * <p>Default value is {@code 64 * 1024}.
     * <p>Mutable at runtime: no
     *
     * @param bytes size in bytes of a single page (byte array) in the LogCache
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCachePageSize(final int bytes) {
        return setSetting(LOG_CACHE_PAGE_SIZE, bytes);
    }

    /**
     * Returns the maximum number of open files that the LogCache maintains in order to reduce system calls to open and
     * close files. The more open files is allowed the less system calls are performed in addition to reading or
     * mapping files. This value can notably affect performance of database warm-up. Default value is {@code 500}.
     * Open files cache is shared amongst all open {@linkplain Environment environments}.
     * <p>Mutable at runtime: no
     *
     * @return maximum number of open files
     */
    public int getLogCacheOpenFilesCount() {
        return (Integer) getSetting(LOG_CACHE_OPEN_FILES);
    }

    /**
     * Sets the maximum number of open files that the LogCache maintains in order to reduce system calls to open and
     * close files. The more open files is allowed the less system calls are performed in addition to reading or
     * mapping files. This value can notably affect performance of database warm-up. Default value is {@code 500}.
     * Open files cache is shared amongst all open {@linkplain Environment environments}.
     * <p>Mutable at runtime: no
     *
     * @param files maximum number of open files
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCacheOpenFilesCount(final int files) {
        return setSetting(LOG_CACHE_OPEN_FILES, files);
    }

    /**
     * Returns {@code true} if any immutable .xd file can be mapped in memory provided there is enough physical memory.
     * On cache miss, LogCache at first tries to check if there is corresponding mapped byte buffer and copies
     * cache page from the buffer, otherwise reads the page from {@linkplain java.io.RandomAccessFile}. If is set to
     * {@code false} then LogCache always reads {@linkplain java.io.RandomAccessFile} on cache miss.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} mapping of .xd files in memory is allowed
     * @see #getLogCacheFreePhysicalMemoryThreshold()
     */
    public boolean getLogCacheUseNio() {
        return (Boolean) getSetting(LOG_CACHE_USE_NIO);
    }

    /**
     * Set {@code true} to allow any immutable .xd file to be mapped in memory provided there is enough physical memory.
     * On cache miss, LogCache at first tries to check if there is corresponding mapped byte buffer and copies
     * cache page from the buffer, otherwise reads the page from {@linkplain java.io.RandomAccessFile}. If is set to
     * {@code false} then LogCache always reads {@linkplain java.io.RandomAccessFile} on cache miss.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @param useNio {@code true} is using NIO is allowed
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCacheUseNio(final boolean useNio) {
        return setSetting(LOG_CACHE_USE_NIO, useNio);
    }

    /**
     * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} defines the minimum size in bytes of free physical memory
     * maintained by the cache memory-mapped files on the host where the JVM runs. On cache miss, LogCache checks if
     * corresponding file is mapped in memory, and if it is not and if free physical memory amount is greater than
     * threshold specified by this setting, tries to map the file in memory. Default value is {@code 1_000_000_000L}
     * bytes, i.e. ~1GB.
     *
     * @return minimum size in bytes of free physical memory
     */
    public long getLogCacheFreePhysicalMemoryThreshold() {
        return (Long) getSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD);
    }

    /**
     * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} sets the minimum size in bytes of free physical memory
     * maintained by the cache memory-mapped files on the host where the JVM runs. On cache miss, LogCache checks if
     * corresponding file is mapped in memory, and if it is not and if free physical memory amount is greater than
     * threshold specified by this setting, tries to map the file in memory. Default value is {@code 1_000_000_000L}
     * bytes, i.e. ~1GB.
     *
     * @param freePhysicalMemoryThreshold minimum size in bytes of free physical memory
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCacheFreePhysicalMemoryThreshold(final long freePhysicalMemoryThreshold) {
        return setSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, freePhysicalMemoryThreshold);
    }

    /**
     * Returns {@code true} if the LogCache is shared. Shared cache caches raw binary
     * data (contents of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
     * By default, the LogCache is shared.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the LogCache is shared
     */
    public boolean isLogCacheShared() {
        return (Boolean) getSetting(LOG_CACHE_SHARED);
    }

    /**
     * Set {@code true} if the LogCache should be shared. Shared cache caches raw binary
     * data (contents of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
     * By default, the LogCache is shared.
     * <p>Mutable at runtime: no
     *
     * @param shared {@code true} if the LogCache should be shared
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCacheShared(final boolean shared) {
        return setSetting(LOG_CACHE_SHARED, shared);
    }

    /**
     * Returns {@code true} if the LogCache should use lock-free data structures. Default value is {@code true}.
     * There is no practical sense to use "blocking" cache, so the setting will be deprecated in future.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the LogCache should use lock-free data structures
     */
    public boolean isLogCacheNonBlocking() {
        return (Boolean) getSetting(LOG_CACHE_NON_BLOCKING);
    }

    /**
     * Set {@code true} if the LogCache should use lock-free data structures. Default value is {@code true}.
     * There is no practical sense to use "blocking" cache, so the setting will be deprecated in future.
     * <p>Mutable at runtime: no
     *
     * @param nonBlocking {@code true} if the LogCache should use lock-free data structures
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCacheNonBlocking(final boolean nonBlocking) {
        return setSetting(LOG_CACHE_NON_BLOCKING, nonBlocking);
    }

    /**
     * Returns {@code true} if the Log constructor fails if the database directory is not clean. Can be useful
     * if an applications expects that the database should always be newly created. Default value is {@code false}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the Log constructor fails if the database directory is not clean
     */
    public boolean isLogCleanDirectoryExpected() {
        return (Boolean) getSetting(LOG_CLEAN_DIRECTORY_EXPECTED);
    }

    /**
     * Set {@code true} if the Log constructor should fail if the database directory is not clean. Can be useful
     * if an applications expects that the database should always be newly created. Default value is {@code false}.
     * <p>Mutable at runtime: no
     *
     * @param logCleanDirectoryExpected {@code true} if the Log constructor should fail if the database directory is not clean
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogCleanDirectoryExpected(final boolean logCleanDirectoryExpected) {
        return setSetting(LOG_CLEAN_DIRECTORY_EXPECTED, logCleanDirectoryExpected);
    }

    /**
     * Returns {@code true} if the Log constructor implicitly clears the database if it occurred to be invalid
     * when opened. Default value is {@code false}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the Log constructor should implicitly clear invalid database
     */
    public boolean isLogClearInvalid() {
        return (Boolean) getSetting(LOG_CLEAR_INVALID);
    }

    /**
     * Set {@code true} if the Log constructor should implicitly clear the database if it occurred to be invalid
     * when opened. Default value is {@code false}.
     * <p>Mutable at runtime: no
     *
     * @param logClearInvalid {@code true} if the Log constructor should implicitly clear invalid database
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setLogClearInvalid(final boolean logClearInvalid) {
        return setSetting(LOG_CLEAR_INVALID, logClearInvalid);
    }

    /**
     * Returns the period in milliseconds to force file system's fsync call that often if {@linkplain #LOG_DURABLE_WRITE}
     * is switched off. Default value is {@code 10000L}.
     * <p>Mutable at runtime: yes
     *
     * @return milliseconds to force file system's fsync call that often
     * @see #getLogDurableWrite()
     */
    public long getLogSyncPeriod() {
        return (Long) getSetting(LOG_SYNC_PERIOD);
    }

    /**
     * Sets the period in milliseconds to force file system's fsync call that often if {@linkplain #LOG_DURABLE_WRITE}
     * is switched off. Default value is {@code 10000L}.
     * <p>Mutable at runtime: yes
     *
     * @param millis milliseconds to force file system's fsync call that often
     * @return this {@code EnvironmentConfig} instance
     * @see #setLogDurableWrite(boolean)
     */
    public EnvironmentConfig setLogSyncPeriod(final long millis) {
        return setSetting(LOG_SYNC_PERIOD, millis);
    }

    /**
     * Returns {@code true} if each complete and immutable {@code Log} file (.xd file) should marked with read-only
     * attribute. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if each complete and immutable .xd file should marked with read-only attribute
     */
    public boolean isLogFullFileReadonly() {
        return (Boolean) getSetting(LOG_FULL_FILE_READ_ONLY);
    }

    /**
     * Set {@code true} if each complete and immutable {@code Log} file (.xd file) should marked with read-only
     * attribute. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @param readonly {@code true} to mark each complete and immutable .xd file with read-only attribute
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setFullFileReadonly(final boolean readonly) {
        return setSetting(LOG_FULL_FILE_READ_ONLY, readonly);
    }

    /**
     * Returns {@code true} if the {@linkplain Environment} instance is read-only. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if the {@linkplain Environment} instance is read-only
     */
    public boolean getEnvIsReadonly() {
        return (Boolean) getSetting(ENV_IS_READONLY);
    }

    /**
     * Set {@code true} to turn the {@linkplain Environment} instance to read-only mode. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @param isReadonly {@code true} to turn the {@linkplain Environment} instance to read-only mode
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvIsReadonly(final boolean isReadonly) {
        return setSetting(ENV_IS_READONLY, isReadonly);
    }

    /**
     * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
     * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a {@linkplain Store},
     * but returns an empty immutable instance instead. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if attempt to create a {@linkplain Store} won't fail immediately in read-only mode
     */
    public boolean getEnvReadonlyEmptyStores() {
        return (Boolean) getSetting(ENV_READONLY_EMPTY_STORES);
    }

    /**
     * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
     * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a {@linkplain Store},
     * but returns an empty immutable instance instead. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @param readonlyEmptyStores {@code true} if attempt to create a {@linkplain Store} shouldn't fail immediately in read-only mode
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvReadonlyEmptyStores(final boolean readonlyEmptyStores) {
        return setSetting(ENV_READONLY_EMPTY_STORES, readonlyEmptyStores);
    }

    /**
     * Returns the size of the "store-get" cache. The "store-get" cache can increase performance of
     * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0} what means that
     * the cache is inactive. If the setting is mutated at runtime the cache is invalidated.
     * <p>Mutable at runtime: yes
     *
     * @return size of the "store-get" cache
     */
    public int getEnvStoreGetCacheSize() {
        return (Integer) getSetting(ENV_STOREGET_CACHE_SIZE);
    }

    /**
     * Sets the size of the "store-get" cache. The "store-get" cache can increase performance of
     * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0} what means that
     * the cache is inactive. If the setting is mutated at runtime the cache is invalidated.
     * <p>Mutable at runtime: yes
     *
     * @param storeGetCacheSize size of the "store-get" cache
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvStoreGetCacheSize(final int storeGetCacheSize) {
        if (storeGetCacheSize < 0) {
            throw new InvalidSettingException("Negative StoreGetCache size");
        }
        return setSetting(ENV_STOREGET_CACHE_SIZE, storeGetCacheSize);
    }

    /**
     * Returns {@code true} if {@linkplain Environment#close()} shouldn't check if there are unfinished
     * transactions. Otherwise it should check and throw {@linkplain ExodusException} if there are.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if {@linkplain Environment#close()} shouldn't check unfinished transactions
     * @see Environment#close()
     */
    public boolean getEnvCloseForcedly() {
        return (Boolean) getSetting(ENV_CLOSE_FORCEDLY);
    }

    /**
     * Set {@code true} if {@linkplain Environment#close()} shouldn't check if there are unfinished
     * transactions. Set {@code false} if it should check and throw {@linkplain ExodusException} if there are unfinished
     * transactions. Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @param closeForcedly {@code true} if {@linkplain Environment#close()} should ignore unfinished transactions
     * @return this {@code EnvironmentConfig} instance
     * @see Environment#close()
     */
    public EnvironmentConfig setEnvCloseForcedly(final boolean closeForcedly) {
        return setSetting(ENV_CLOSE_FORCEDLY, closeForcedly);
    }

    /**
     * Returns the number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2000L}.
     * <p>Mutable at runtime: yes
     *
     * @return number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * @see Transaction
     * @see #getEnvTxnReplayMaxCount()
     */
    public long getEnvTxnReplayTimeout() {
        return (Long) getSetting(ENV_TXN_REPLAY_TIMEOUT);
    }

    /**
     * Sets the number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2000L}.
     * <p>Mutable at runtime: yes
     *
     * @param timeout number of millisecond which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * @return this {@code EnvironmentConfig} instance
     * @see Transaction
     * @see #setEnvTxnReplayMaxCount(int)
     */
    public EnvironmentConfig setEnvTxnReplayTimeout(final long timeout) {
        if (timeout < 0) {
            throw new InvalidSettingException("Negative transaction replay timeout");
        }
        return setSetting(ENV_TXN_REPLAY_TIMEOUT, timeout);
    }

    /**
     * Returns the number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     *
     * @return number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * @see Transaction
     * @see #getEnvTxnReplayTimeout()
     */
    public int getEnvTxnReplayMaxCount() {
        return (Integer) getSetting(ENV_TXN_REPLAY_MAX_COUNT);
    }

    /**
     * Sets the number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * (switch to an exclusive mode). Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     *
     * @param count number of times which a {@linkplain Transaction} can try to flush without attempts to upgrade
     * @return this {@code EnvironmentConfig} instance
     * @see Transaction
     * @see #setEnvTxnReplayTimeout(long)
     */
    public EnvironmentConfig setEnvTxnReplayMaxCount(final int count) {
        if (count < 0) {
            throw new InvalidSettingException("Negative transaction replay count");
        }
        return setSetting(ENV_TXN_REPLAY_MAX_COUNT, count);
    }

    /**
     * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself after
     * {@linkplain Transaction#flush()}. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if any upgraded {@linkplain Transaction} will downgrade itself after flush
     */
    public boolean getEnvTxnDowngradeAfterFlush() {
        return (Boolean) getSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH);
    }

    /**
     * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself after
     * {@linkplain Transaction#flush()}. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @param downgrade {@code true} if any upgraded {@linkplain Transaction} will downgrade itself after flush
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvTxnDowngradeAfterFlush(final boolean downgrade) {
        return setSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH, downgrade);
    }

    /**
     * Returns the number of {@linkplain Transaction transactions} that can be started in parallel. By default it is
     * unlimited.
     * <p>Mutable at runtime: no
     *
     * @return number of {@linkplain Transaction transactions} that can be started in parallel
     */
    public int getEnvMaxParallelTxns() {
        return (Integer) getSetting(ENV_MAX_PARALLEL_TXNS);
    }

    /**
     * Sets the number of {@linkplain Transaction transactions} that can be started in parallel. By default it is
     * unlimited.
     * <p>Mutable at runtime: no
     *
     * @param maxParallelTxns number of {@linkplain Transaction transactions} that can be started in parallel
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvMaxParallelTxns(final int maxParallelTxns) {
        return setSetting(ENV_MAX_PARALLEL_TXNS, maxParallelTxns);
    }

    /**
     * Returns the number of read-only {@linkplain Transaction transactions} that can be started in parallel. By
     * default it is unlimited.
     * <p>Mutable at runtime: no
     *
     * @return number of read-only {@linkplain Transaction transactions} that can be started in parallel
     */
    public int getEnvMaxParallelReadonlyTxns() {
        return (Integer) getSetting(ENV_MAX_PARALLEL_READONLY_TXNS);
    }

    /**
     * Sets the number of read-only {@linkplain Transaction transactions} that can be started in parallel. By
     * default it is unlimited.
     * <p>Mutable at runtime: no
     *
     * @param maxParallelReadonlyTxns number of read-only {@linkplain Transaction transactions} that can be started in parallel
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setEnvMaxParallelReadonlyTxns(final int maxParallelReadonlyTxns) {
        return setSetting(ENV_MAX_PARALLEL_TXNS, maxParallelReadonlyTxns);
    }

    /**
     * Returns {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this timeout then
     * it is reported in logs as stuck along with stack trace which it was created with. Default value is {@code 0}
     * which means that no timeout for a {@linkplain Transaction} is defined. In that case, no monitor of stuck
     * transactions is started. Otherwise it is started for each {@linkplain Environment}, though consuming only a
     * single {@linkplain Thread} amongst all environments created within a single class loader.
     * <p>Mutable at runtime: no
     *
     * @return timeout of a {@linkplain Transaction} in milliseconds
     * @see #getEnvMonitorTxnsCheckFreq()
     */
    public int getEnvMonitorTxnsTimeout() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_TIMEOUT);
    }

    /**
     * Sets {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this timeout then
     * it is reported in logs as stuck along with stack trace which it was created with. Default value is {@code 0}
     * which means that no timeout for a {@linkplain Transaction} is defined. In that case, no monitor of stuck
     * transactions is started. Otherwise it is started for each {@linkplain Environment}, though consuming only a
     * single {@linkplain Thread} amongst all environments created within a single class loader.
     * <p>Mutable at runtime: no
     *
     * @param timeout timeout of a {@linkplain Transaction} in milliseconds
     * @return this {@code EnvironmentConfig} instance
     * @see #setEnvMonitorTxnsCheckFreq(int)
     */
    public EnvironmentConfig setEnvMonitorTxnsTimeout(final int timeout) {
        if (timeout != 0 && timeout < 1000) {
            throw new InvalidSettingException("Transaction timeout should be greater than a second");
        }
        setSetting(ENV_MONITOR_TXNS_TIMEOUT, timeout);
        if (timeout > 0 && timeout < getEnvMonitorTxnsCheckFreq()) {
            setEnvMonitorTxnsCheckFreq(timeout);
        }
        return this;
    }

    /**
     * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts and checks
     * {@linkplain Environment}'s transactions with this frequency (period) specified in milliseconds.
     * Default value is {@code 60000}, one minute.
     * <p>Mutable at runtime: no
     *
     * @return frequency (period) in milliseconds of checking stuck transactions
     * @see #getEnvMonitorTxnsTimeout()
     */
    public int getEnvMonitorTxnsCheckFreq() {
        return (Integer) getSetting(ENV_MONITOR_TXNS_CHECK_FREQ);
    }

    /**
     * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts and checks
     * {@linkplain Environment}'s transactions with this frequency (period) specified in milliseconds.
     * Default value is {@code 60000}, one minute.
     * <p>Mutable at runtime: no
     *
     * @param freq frequency (period) in milliseconds of checking stuck transactions
     * @return this {@code EnvironmentConfig} instance
     * @see #setEnvMonitorTxnsTimeout(int)
     */
    public EnvironmentConfig setEnvMonitorTxnsCheckFreq(final int freq) {
        return setSetting(ENV_MONITOR_TXNS_CHECK_FREQ, freq);
    }

    /**
     * Returns {@code true} if the {@linkplain Environment} gathers statistics. If
     * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX managed bean.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the {@linkplain Environment} gathers statistics
     * @see Environment#getStatistics()
     */
    public boolean getEnvGatherStatistics() {
        return (Boolean) getSetting(ENV_GATHER_STATISTICS);
    }

    /**
     * Set {@code true} if the {@linkplain Environment} should gather statistics. If
     * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX managed bean.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @param gatherStatistics {@code true} if the {@linkplain Environment} should gather statistics
     * @return this {@code EnvironmentConfig} instance
     * @see Environment#getStatistics()
     */
    public EnvironmentConfig setEnvGatherStatistics(final boolean gatherStatistics) {
        return setSetting(ENV_GATHER_STATISTICS, gatherStatistics);
    }

    /**
     * Returns the maximum size of page of B+Tree. Default value is {@code 128}.
     * <p>Mutable at runtime: no
     *
     * @return maximum size of page of B+Tree
     */
    public int getTreeMaxPageSize() {
        return (Integer) getSetting(TREE_MAX_PAGE_SIZE);
    }

    /**
     * Sets the maximum size of page of B+Tree. Default value is {@code 128}. Only sizes in the range [16..1024]
     * are accepted.
     * <p>Mutable at runtime: no
     *
     * @param pageSize maximum size of page of B+Tree
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException page size is not in the range [16..1024]
     */
    public EnvironmentConfig setTreeMaxPageSize(final int pageSize) throws InvalidSettingException {
        if (pageSize < 16 || pageSize > 1024) {
            throw new InvalidSettingException("Invalid tree page size: " + pageSize);
        }
        return setSetting(TREE_MAX_PAGE_SIZE, pageSize);
    }

    /**
     * As of 1.0.5, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     *
     * @return {@code 0}
     */
    @Deprecated
    public int getTreeNodesCacheSize() {
        return 0;
    }

    /**
     * As of 1.0.5, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     *
     * @return this {@code EnvironmentConfig} instance
     */
    @Deprecated
    public EnvironmentConfig setTreeNodesCacheSize(final int cacheSize) {
        return this;
    }

    /**
     * Returns {@code true} if the database garbage collector is enabled. Default value is {@code true}.
     * Switching GC off makes sense only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if the database garbage collector is enabled
     */
    public boolean isGcEnabled() {
        return (Boolean) getSetting(GC_ENABLED);
    }

    /**
     * Set {@code true} to enable the database garbage collector. Default value is {@code true}.
     * Switching GC off makes sense only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     *
     * @param enabled {@code true} to enable the database garbage collector
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setGcEnabled(boolean enabled) {
        return setSetting(GC_ENABLED, enabled);
    }

    /**
     * Returns the number of milliseconds which the database garbage collector is postponed for after the
     * {@linkplain Environment} is created. Default value is {@code 10000}.
     * <p>Mutable at runtime: no
     *
     * @return number of milliseconds which the database garbage collector is postponed for after the
     * {@linkplain Environment} is created
     */
    public int getGcStartIn() {
        return (Integer) getSetting(GC_START_IN);
    }

    /**
     * Sets the number of milliseconds which the database garbage collector is postponed for after the
     * {@linkplain Environment} is created. Default value is {@code 10000}.
     * <p>Mutable at runtime: no
     *
     * @param startInMillis number of milliseconds which the database garbage collector should be postponed for after the
     *                      {@linkplain Environment} is created
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException {@code startInMillis} is negative
     */
    public EnvironmentConfig setGcStartIn(final int startInMillis) throws InvalidSettingException {
        if (startInMillis < 0) {
            throw new InvalidSettingException("GC can't be postponed for that number of milliseconds: " + startInMillis);
        }
        return setSetting(GC_START_IN, startInMillis);
    }

    /**
     * Returns percent of minimum database utilization. Default value is {@code 50}. That means that 50 percent
     * of free space in raw data in {@code Log} files (.xd files) is allowed. If database utilization is less than
     * defined (free space percent is more than {@code 50}), the database garbage collector is triggered.
     * <p>Mutable at runtime: yes
     *
     * @return percent of minimum database utilization
     */
    public int getGcMinUtilization() {
        return (Integer) getSetting(GC_MIN_UTILIZATION);
    }

    /**
     * Sets percent of minimum database utilization. Default value is {@code 50}. That means that 50 percent
     * of free space in raw data in {@code Log} files (.xd files) is allowed. If database utilization is less than
     * defined (free space percent is more than {@code 50}), the database garbage collector is triggered.
     * <p>Mutable at runtime: yes
     *
     * @param percent percent of minimum database utilization
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException {@code percent} is not in the range [1..90]
     */
    public EnvironmentConfig setGcMinUtilization(int percent) throws InvalidSettingException {
        if (percent < 1 || percent > 90) {
            throw new InvalidSettingException("Invalid minimum log files utilization: " + percent);
        }
        return setSetting(GC_MIN_UTILIZATION, percent);
    }

    /**
     * If returns {@code true} the database garbage collector renames files rather than deletes them. Default
     * value is {@code false}. It makes sense to change this setting only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if the database garbage collector should rename files rather than deletes them
     */
    public boolean getGcRenameFiles() {
        return (Boolean) getSetting(GC_RENAME_FILES);
    }

    /**
     * Set {@code true} if the database garbage collector should rename files rather than deletes them. Default
     * value is {@code false}. It makes sense to change this setting only for debugging and troubleshooting purposes.
     * <p>Mutable at runtime: yes
     *
     * @param rename {@code true} if the database garbage collector should rename files rather than deletes them
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setGcRenameFiles(boolean rename) {
        return setSetting(GC_RENAME_FILES, rename);
    }

    /**
     * As of 1.0.2, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     *
     * @return {@code false}
     */
    @Deprecated
    public boolean getGcUseExpirationChecker() {
        return false;
    }

    /**
     * As of 1.0.2, is deprecated and has no effect.
     * <p>Mutable at runtime: no
     *
     * @return this {@code EnvironmentConfig} instance
     */
    @Deprecated
    public EnvironmentConfig setGcUseExpirationChecker(boolean useExpirationChecker) {
        return this;
    }

    /**
     * Returns the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the database garbage
     * collector. The age of the last (the newest, the rightmost) {@code Log} file is {@code 0}, the age of previous
     * file is {@code 1}, etc. Default value is {@code 2}.
     * <p>Mutable at runtime: yes
     *
     * @return minimum age of .xd file to consider it for cleaning by the database garbage collector
     */
    public int getGcFileMinAge() {
        return (Integer) getSetting(GC_MIN_FILE_AGE);
    }

    /**
     * Returns the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the database garbage
     * collector. The age of the last (the newest, the rightmost) {@code Log} file is {@code 0}, the age of previous
     * file is {@code 1}, etc. Default value is {@code 2}. The age cannot be less than {@code 1}.
     * <p>Mutable at runtime: yes
     *
     * @param minAge minimum age of .xd file to consider it for cleaning by the database garbage collector
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException minAge is less than {@code 1}.
     */
    public EnvironmentConfig setGcFileMinAge(int minAge) throws InvalidSettingException {
        if (minAge < 1) {
            throw new InvalidSettingException("Invalid file minimum age: " + minAge);
        }
        return setSetting(GC_MIN_FILE_AGE, minAge);
    }

    /**
     * Returns the number of new {@code Log} files (.xd files) that must be created to trigger if necessary (if database
     * utilization is not sufficient) the next background cleaning cycle (single run of the database garbage collector)
     * after the previous cycle finished. Default value is {@code 3}, i.e. GC can start after each 3 newly created
     * {@code Log} files.
     * <p>Mutable at runtime: yes
     *
     * @return number of new .xd files that must be created to trigger the next background cleaning cycle
     */
    public int getGcFilesInterval() {
        return (Integer) getSetting(GC_FILES_INTERVAL);
    }

    /**
     * Sets the number of new {@code Log} files (.xd files) that must be created to trigger if necessary (if database
     * utilization is not sufficient) the next background cleaning cycle (single run of the database garbage collector)
     * after the previous cycle finished. Default value is {@code 3}, i.e. GC can start after each 3 newly created
     * {@code Log} files. Cannot be less than {@code 1}.
     * <p>Mutable at runtime: yes
     *
     * @param files number of new .xd files that must be created to trigger the next background cleaning cycle
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException {@code files} is less than {@code 1}
     */
    public EnvironmentConfig setGcFilesInterval(final int files) throws InvalidSettingException {
        if (files < 1) {
            throw new InvalidSettingException("Invalid number of files: " + files);
        }
        return setSetting(GC_FILES_INTERVAL, files);
    }

    /**
     * Returns the number of milliseconds after which background cleaning cycle (single run of the database garbage
     * collector) can be repeated if the previous one didn't reach required utilization. Default value is
     * {@code 30000}.
     * <p>Mutable at runtime: yes
     *
     * @return number of milliseconds after which background cleaning cycle can be repeated if the previous one
     * didn't reach required utilization
     */
    public int getGcRunPeriod() {
        return (Integer) getSetting(GC_RUN_PERIOD);
    }

    /**
     * Sets the number of milliseconds after which background cleaning cycle (single run of the database garbage
     * collector) can be repeated if the previous one didn't reach required utilization. Default value is
     * {@code 30000}.
     * <p>Mutable at runtime: yes
     *
     * @param runPeriod number of milliseconds after which background cleaning cycle can be repeated if the previous one
     *                  didn't reach required utilization
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException {@code runPeriod} is less than {@code 0}
     */
    public EnvironmentConfig setGcRunPeriod(final int runPeriod) throws InvalidSettingException {
        if (runPeriod < 0) {
            throw new InvalidSettingException("Invalid GC run period: " + runPeriod);
        }
        return setSetting(GC_RUN_PERIOD, runPeriod);
    }

    /**
     * Returns {@code true} if database utilization will be computed from scratch before the first cleaning
     * cycle (single run of the database garbage collector) is triggered, i.e. shortly after the database is open.
     * In addition, can be used to compute utilization information at runtime by just modifying the setting value.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if database utilization will be computed from scratch
     */
    public boolean getGcUtilizationFromScratch() {
        return (Boolean) getSetting(GC_UTILIZATION_FROM_SCRATCH);
    }

    /**
     * Set {@code true} if database utilization should be computed from scratch before the first cleaning
     * cycle (single run of the database garbage collector) is triggered, i.e. shortly after the database is open.
     * In addition, can be used to compute utilization information at runtime by just modifying the setting value.
     * Default value is {@code false}.
     * <p>Mutable at runtime: yes
     *
     * @param fromScratch {@code true} if database utilization should be computed from scratch
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setGcUtilizationFromScratch(final boolean fromScratch) {
        return setSetting(GC_UTILIZATION_FROM_SCRATCH, fromScratch);
    }

    /**
     * Returns full path to the file with stored utilization. Is used on creation of an
     * {@linkplain Environment} to update {@code .xd} files' utilization before the first cleaning
     * cycle (single run of the database garbage collector) is triggered. In addition, can be used to reload utilization
     * information at runtime by just modifying the setting value. Format of the stored utilization is expected
     * to be the same as created by the {@code "-d"} option of the {@code Reflect} tool.
     * Default value is empty string.
     * <p>Mutable at runtime: yes
     *
     * @return if not empty, full path to the file with stored utilization.
     */
    public String getGcUtilizationFromFile() {
        return (String) getSetting(GC_UTILIZATION_FROM_FILE);
    }

    /**
     * Sets full path to the file with stored utilization. Is used on creation of an
     * {@linkplain Environment} to update {@code .xd} files' utilization before the first cleaning
     * cycle (single run of the database garbage collector) is triggered. In addition, can be used to reload utilization
     * information at runtime by just modifying the setting value. Format of the stored utilization is expected
     * to be the same as created by the {@code "-d"} option of the {@code Reflect} tool.
     * Default value is empty string.
     * <p>Mutable at runtime: yes
     *
     * @param file full path to the file with stored utilization
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setGcUtilizationFromFile(final String file) {
        return setSetting(GC_UTILIZATION_FROM_FILE, file);
    }

    /**
     * Returns {@code true} if the database garbage collector tries to acquire exclusive {@linkplain Transaction}
     * for its purposes. In that case, GC transaction never re-plays. In order to not block background cleaner thread
     * forever, acquisition of exclusive GC transaction is performed with a timeout controlled by the
     * {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT} setting. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @return {@code true} if the database garbage collector tries to acquire exclusive {@linkplain Transaction}
     * @see #getGcTransactionAcquireTimeout()
     * @see #getGcTransactionTimeout()
     */
    public boolean getGcUseExclusiveTransaction() {
        return (Boolean) getSetting(GC_USE_EXCLUSIVE_TRANSACTION);
    }

    /**
     * Sets {@code true} if the database garbage collector should try to acquire exclusive {@linkplain Transaction}
     * for its purposes. In that case, GC transaction never re-plays. In order to not block background cleaner thread
     * forever, acquisition of exclusive GC transaction is performed with a timeout controlled by the
     * {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT} setting. Default value is {@code true}.
     * <p>Mutable at runtime: yes
     *
     * @param useExclusiveTransaction {@code true} if the database garbage collector should try to acquire exclusive {@linkplain Transaction}
     * @return this {@code EnvironmentConfig} instance
     * @see #setGcTransactionAcquireTimeout(int)
     * @see #setGcTransactionTimeout(int)
     */
    public EnvironmentConfig setGcUseExclusiveTransaction(final boolean useExclusiveTransaction) {
        return setSetting(GC_USE_EXCLUSIVE_TRANSACTION, useExclusiveTransaction);
    }

    /**
     * Returns timeout in milliseconds which is used by the database garbage collector to acquire exclusive
     * {@linkplain Transaction} for its purposes if {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @return timeout in milliseconds which is used by the database garbage collector to acquire exclusive {@linkplain Transaction}
     * @see #getGcUseExclusiveTransaction()
     * @see #getGcTransactionTimeout()
     */
    public int getGcTransactionAcquireTimeout() {
        return (Integer) getSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT);
    }

    /**
     * Sets timeout in milliseconds which is used by the database garbage collector to acquire exclusive
     * {@linkplain Transaction} for its purposes if {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @param txnAcquireTimeout timeout in milliseconds which is used by the database garbage collector to acquire exclusive {@linkplain Transaction}
     * @return this {@code EnvironmentConfig} instance
     * @see #setGcUseExclusiveTransaction(boolean)
     * @see #setGcTransactionTimeout(int)
     */
    public EnvironmentConfig setGcTransactionAcquireTimeout(final int txnAcquireTimeout) {
        return setSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT, txnAcquireTimeout);
    }

    /**
     * Returns timeout in milliseconds which is used by the database garbage collector to reclaim non-expired data
     * in several files inside single GC {@linkplain Transaction} acquired exclusively.
     * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @return timeout in milliseconds which is used by the database garbage collector to reclaim non-expired data
     * in several files inside single {@linkplain Transaction} acquired exclusively
     * @see #getGcUseExclusiveTransaction()
     * @see #getGcTransactionAcquireTimeout()
     */
    public int getGcTransactionTimeout() {
        return (Integer) getSetting(GC_TRANSACTION_TIMEOUT);
    }

    /**
     * Sets timeout in milliseconds which is used by the database garbage collector to reclaim non-expired data
     * in several files inside single GC {@linkplain Transaction} acquired exclusively.
     * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}.
     * Default value is {@code 1000}.
     * <p>Mutable at runtime: yes
     *
     * @param txnTimeout timeout in milliseconds which is used by the database garbage collector to reclaim non-expired data
     *                   in several files inside single {@linkplain Transaction} acquired exclusively
     * @return this {@code EnvironmentConfig} instance
     * @see #setGcUseExclusiveTransaction(boolean)
     * @see #setGcTransactionAcquireTimeout(int)
     */
    public EnvironmentConfig setGcTransactionTimeout(final int txnTimeout) {
        return setSetting(GC_TRANSACTION_TIMEOUT, txnTimeout);
    }

    /**
     * Returns the number of milliseconds which deletion of any successfully cleaned {@code Log} file (.xd file)
     * is postponed for. Default value is {@code 5000}.
     * <p>Mutable at runtime: yes
     *
     * @return number of milliseconds which deletion of any successfully cleaned .xd file is postponed for
     */
    public int getGcFilesDeletionDelay() {
        return (Integer) getSetting(GC_FILES_DELETION_DELAY);
    }

    /**
     * Sets the number of milliseconds which deletion of any successfully cleaned {@code Log} file (.xd file)
     * is postponed for. Default value is {@code 5000}.
     * <p>Mutable at runtime: yes
     *
     * @param delay number of milliseconds which deletion of any successfully cleaned .xd file is postponed for
     * @return this {@code EnvironmentConfig} instance
     * @throws InvalidSettingException {@code delay} is less than {@code 0}.
     */
    public EnvironmentConfig setGcFilesDeletionDelay(final int delay) throws InvalidSettingException {
        if (delay < 0) {
            throw new InvalidSettingException("Invalid GC files deletion delay: " + delay);
        }
        return setSetting(GC_FILES_DELETION_DELAY, delay);
    }

    /**
     * Return {@code true} if the {@linkplain Environment} exposes two JMX managed beans. One for
     * {@linkplain Environment#getStatistics() environment statistics} and second for controlling the
     * {@code EnvironmentConfig} settings. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if the {@linkplain Environment} exposes JMX managed beans
     */
    public boolean isManagementEnabled() {
        return (Boolean) getSetting(MANAGEMENT_ENABLED);
    }

    /**
     * Set {@code true} if the {@linkplain Environment} should expose two JMX managed beans. One for
     * {@linkplain Environment#getStatistics() environment statistics} and second for controlling the
     * {@code EnvironmentConfig} settings. Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @param managementEnabled {@code true} if the {@linkplain Environment} should expose JMX managed beans
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setManagementEnabled(final boolean managementEnabled) {
        return setSetting(MANAGEMENT_ENABLED, managementEnabled);
    }

    /**
     * If is set to {@code true} then exposed JMX managed beans cannot have operations.
     * Default value is {@code true}.
     * <p>Mutable at runtime: no
     *
     * @return {@code true} if exposed JMX managed beans cannot have operations.
     * @see #isManagementEnabled()
     */
    public boolean getManagementOperationsRestricted() {
        return (Boolean) getSetting(MANAGEMENT_OPERATIONS_RESTRICTED);
    }

    /**
     * If {@linkplain #isManagementEnabled()} then set {@code false} in order to expose operations with JMX managed
     * beans in addition to attributes.
     *
     * @param operationsRestricted {@code false} if JMX managed beans should expose operations in addition to attributes
     * @return this {@code EnvironmentConfig} instance
     */
    public EnvironmentConfig setManagementOperationsRestricted(final boolean operationsRestricted) {
        return setSetting(MANAGEMENT_OPERATIONS_RESTRICTED, operationsRestricted);
    }
}
