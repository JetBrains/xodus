/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.env;

import jetbrains.exodus.*;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.crypto.KryptKt;
import jetbrains.exodus.crypto.StreamCipher;
import jetbrains.exodus.crypto.StreamCipherProvider;
import jetbrains.exodus.entitystore.MetaServer;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataReaderWriterProvider;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.StorageTypeNotAllowedException;
import jetbrains.exodus.system.JVMConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Specifies settings of {@linkplain Environment}. Default settings are specified by
 * {@linkplain #DEFAULT} which is immutable. Any newly created {@code EnvironmentConfig} has the
 * same settings as {@linkplain #DEFAULT}.
 *
 * <p>As a rule, the {@code EnvironmentConfig} instance is created along with the
 * {@linkplain Environment} one.
 * E.g., creation of {@linkplain Environment} with some tuned garbage collector settings can look as
 * follows:
 * <pre>
 *     final EnvironmentConfig config = new EnvironmentConfig();
 *     final Environment env = Environments.newInstance(""dbDirectory", config.setGcFileMinAge(3).setGcRunPeriod(60000));
 * </pre>
 * <p>
 * Some setting are mutable at runtime and some are immutable. Immutable at runtime settings can be
 * changed, but they won't take effect on the {@linkplain Environment} instance. Those settings are
 * applicable only during {@linkplain Environment} instance creation.
 *
 * <p>Some settings can be changed only before the database is physically created on storage device.
 * E.g.,
 * {@linkplain #LOG_FILE_SIZE} defines the size of a single {@code Log} file (.xd file) which cannot
 * be changed for existing databases. It makes sense to choose values of such settings at design
 * time and hardcode them.
 *
 * <p>You can define custom processing of changed settings values by
 * {@linkplain #addChangedSettingsListener(ConfigSettingChangeListener)}. Override
 * {@linkplain ConfigSettingChangeListener#beforeSettingChanged(String, Object, Map)} to pre-process
 * mutations of settings and
 * {@linkplain ConfigSettingChangeListener#afterSettingChanged(String, Object, Map)} to post-process
 * them.
 *
 * @see Environment
 * @see Environment#getEnvironmentConfig()
 */
@SuppressWarnings({"WeakerAccess", "unused", "AutoBoxing", "AutoUnboxing"})
public class EnvironmentConfig extends AbstractConfig {

    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig(
            ConfigurationStrategy.IGNORE) {
        @Override
        public EnvironmentConfig setMutable(boolean isMutable) {
            if (!this.isMutable() && isMutable) {
                throw new ExodusException("Can't make EnvironmentConfig.DEFAULT mutable");
            }
            return super.setMutable(isMutable);
        }
    }.setMutable(false);

    /**
     * Defines absolute value of memory in bytes that can be used by the LogCache. By default, is not
     * set.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE_PERCENTAGE
     */
    public static final String MEMORY_USAGE = "exodus.memoryUsage";

    /**
     * Defines percent of max memory (specified by the "-Xmx" java parameter) that can be used by the
     * LogCache. Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is
     * {@code 50}.
     * <p>Mutable at runtime: no
     *
     * @see #MEMORY_USAGE
     */
    public static final String MEMORY_USAGE_PERCENTAGE = "exodus.memoryUsagePercentage";

    public static final String USE_VERSION1_FORMAT = "exodus.useVersion1Format";

    /**
     * Xodus performs check of consistency of pages loaded from disk. This option allows to
     * enable/disable this check.
     * <p>
     * Disabling this check should improve general performance of database but decreases durability
     * guaranties.
     * <p>
     * Default value is {@code true}.
     *
     * <p>Mutable at runtime: no</p>
     */
    public static final String CHECK_PAGES_AT_RUNTIME = "exodus.checkPagesAtRuntime";

    /**
     * Defines id of {@linkplain StreamCipherProvider} which will be used to encrypt the database.
     * Default value is {@code null}, which means that the database won't be encrypted. The setting
     * cannot be changed for existing databases. Default value is {@code null}.
     * <p>Mutable at runtime: no
     *
     * @see #CIPHER_KEY
     * @see #CIPHER_BASIC_IV
     * @see StreamCipher
     * @see StreamCipherProvider
     */
    public static final String CIPHER_ID = "exodus.cipherId";

    /**
     * Defines the key which will be used to encrypt the database. The key is expected to be a hex
     * string representing a byte array which is passed to
     * {@linkplain StreamCipher#init(byte[], long)}. Is applicable only if {@linkplain #CIPHER_ID} is
     * not {@code null}. The setting cannot be changed for existing databases. Default value is
     * {@code null}.
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
     * Defines basic IV (initialization vector) which will be used to encrypt the database. Basic IV
     * is expected to be random (pseudo-random) and unique long value. Basic IV is used to calculate
     * relative IVs which are passed to {@linkplain StreamCipher#init(byte[], long)}. Is applicable
     * only if {@linkplain #CIPHER_ID} is not {@code null}. The setting cannot be changed for existing
     * databases. Default value is {@code 0L}.
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
     * If is set to {@code true} database profiler is enabled. By default, it is disabled.
     * <p>Mutable at runtime: no
     *
     * @since 1.4.0
     */
    public static final String PROFILER_ENABLED = "exodus.profiler.enabled";

    /**
     * If is set to {@code true} forces file system's fsync call after each committed or flushed
     * transaction. By default, is switched off since it creates great performance overhead and can be
     * controlled manually.
     * <p>Mutable at runtime: yes
     */
    public static final String LOG_DURABLE_WRITE = "exodus.log.durableWrite";

    /**
     * Defines the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting
     * cannot be changed for existing databases. Default value is {@code 8192L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_FILE_SIZE = "exodus.log.fileSize";

    /**
     * Defines the number of milliseconds the Log constructor waits for the lock file. Default value
     * is {@code 0L}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_LOCK_TIMEOUT = "exodus.log.lockTimeout";

    /**
     * Defines the debug identifier to be written to the lock file alongside with other debug
     * information. Default value is {@code ManagementFactory.getRuntimeMXBean().getName()} which has
     * a form of {@code pid@hostname}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_LOCK_ID = "exodus.log.lockID";

    /**
     * Defines the size in bytes of a single page (byte array) in the LogCache. This number of bytes
     * is read from {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at
     * a time.
     *
     * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s
     * should be configured
     * to use single LogCache page size.
     *
     * <p>Default value is {@code 64 * 1024}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_PAGE_SIZE = "exodus.log.cache.pageSize";

    /**
     * Defines the maximum number of open files that LogCache maintains in order to reduce system
     * calls to open and close files. The more open files is allowed the less system calls are
     * performed in addition to reading or mapping files. This value can notably affect performance of
     * database warm-up. Default value is {@code 500}. Open files cache is shared amongst all open
     * {@linkplain Environment environments}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_OPEN_FILES = "exodus.log.cache.openFilesCount";

    /**
     * If is set to {@code true} any immutable file can be mapped in memory provided there is enough
     * physical memory. On cache miss, LogCache at first tries to check if there is corresponding
     * mapped byte buffer and copies cache page from the buffer, otherwise reads the page from
     * {@linkplain java.io.RandomAccessFile}. If is set to {@code false} then LogCache always reads
     * {@linkplain java.io.RandomAccessFile} on cache miss. Default value was {@code true} before
     * version {@code 1.2.3}. As of {@code 1.2.3}, default value is {@code false}.
     *
     * <p>Mutable at runtime: no
     *
     * @see #LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD
     * @deprecated Because of upcoming release of virtual threads feature this property is deprecated.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public static final String LOG_CACHE_USE_NIO = "exodus.log.cache.useNIO";

    /**
     * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} defines the minimum size in bytes of free
     * physical memory maintained by the cache memory-mapped files on the host where the JVM runs. On
     * cache miss, LogCache checks if corresponding file is mapped in memory, and if it is not and if
     * free physical memory amount is greater than threshold specified by this setting, tries to map
     * the file in memory. Default value is {@code 1_000_000_000L} bytes, i.e. ~1GB.
     * <p>Mutable at runtime: no
     *
     * @see #LOG_CACHE_USE_NIO
     * @deprecated Because of upcoming release of virtual threads feature this property is deprecated.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public static final String LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD = "exodus.log.cache.freePhysicalMemoryThreshold";

    /**
     * If is set to {@code true} the LogCache is shared. Shared cache caches raw binary data (contents
     * of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
     * By default, the LogCache is shared.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_SHARED = "exodus.log.cache.shared";

    /**
     * If is set to {@code true} the LogCache uses lock-free data structures. Default value is
     * {@code true}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_NON_BLOCKING = "exodus.log.cache.nonBlocking";

    /**
     * Defines the number of generations of non-blocking LogCache. Is applicable only if
     * {@linkplain #LOG_CACHE_NON_BLOCKING} is set to {@code true}. The higher number of generations
     * is, the higher the cache hit rate is and CPU ticks necessary to get a single page from the
     * cache. Default value is {@code 2}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_GENERATION_COUNT = "exodus.log.cache.generationCount";

    /**
     * If is set to {@code true} LogCache uses
     * {@linkplain java.lang.ref.SoftReference soft references} for holding cached pages. The cache
     * still uses not more memory than it is configured by {@linkplain #MEMORY_USAGE} or
     * {@linkplain #MEMORY_USAGE_PERCENTAGE} settings, but JVM GC can reclaim memory used by the cache
     * on a heavy load surge. On the other hand, use of soft references results in greater JVM GC load
     * and greater general CPU consumption by the cache. So one can choose either memory-flexible, or
     * CPU-optimal cache. Default value is {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_USE_SOFT_REFERENCES = "exodus.log.cache.useSoftReferences";

    /**
     * Defines the number of successive pages to be read at once in case of LogCache miss. Reading
     * successive pages can reduce amount of random access to database files. It can be useful in
     * workloads like application warm-up. Default value is {@code 1} which means that no read-ahead
     * strategy is applied.
     * <p>Mutable at runtime: yes
     */
    public static final String LOG_CACHE_READ_AHEAD_MULTIPLE = "exodus.log.cache.readAheadMultiple";

    /**
     * If is set to {@code true} LogCache will populate itself with database file pages right after
     * the database is opened using this {@code EnvironmentConfig} instance. Default value is
     * {@code false}.
     * <p>Mutable at runtime: no
     */
    public static final String LOG_CACHE_WARMUP = "exodus.log.cache.warmup";

  /**
   * If is set to {@code true} then the Log constructor fails if the database directory is not
   * clean. Can be useful if an applications expects that the database should always be newly
   * created. Default value is {@code false}.
   * <p>Mutable at runtime: no
   */
  public static final String LOG_CLEAN_DIRECTORY_EXPECTED = "exodus.log.cleanDirectoryExpected";

  /**
   * If is set to {@code true} then the Log constructor implicitly clears the database if it
   * occurred to be invalid when opened. Default value is {@code false}.
   * <p>Mutable at runtime: no
   */
  public static final String LOG_CLEAR_INVALID = "exodus.log.clearInvalid";

  public static final String LOG_SKIP_INVALID_LOGGALE_TYPE = "exodus.log.skipInvalidLoggableType";

  /**
   * Sets the period in milliseconds to force file system's fsync call that often if
   * {@linkplain #LOG_DURABLE_WRITE} is switched off. Default value is {@code 10000L}.
   * <p>Mutable at runtime: yes
   *
   * @see #LOG_DURABLE_WRITE
   */
  public static final String LOG_SYNC_PERIOD = "exodus.log.syncPeriod";

  /**
   * Forces to check data consistency of the database on opening. Default value is {@code false}.
   *
   * <p>Mutable at runtime: no
   */
  public static final String LOG_FORCE_CHECK_DATA_CONSISTENCY = "exodus.log.forceCheckDataConsistency";

  /**
   * Forces data restore routine to proceed even if it is not possible to restore all the data.
   * Default value is {@code false}.
   *
   * <p>Mutable at runtime: no
   */
  public static final String LOG_PROCEED_DATA_RESTORE_AT_ANY_COST = "exodus.log.proceedDataRestoreAtAnyCost";

  /**
   * If is set to {@code true} then each complete and immutable {@code Log} file (.xd file) is
   * marked with read-only attribute. Default value is {@code true}.
   * <p>Mutable at runtime: no
   */
  public static final String LOG_FULL_FILE_READ_ONLY = "exodus.log.fullFileReadonly";

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a removable storage. Attempt to open database
   * on a storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}.
   * Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @since 1.4.0
   */
  public static final String LOG_ALLOW_REMOVABLE = "exodus.log.allowRemovable";

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a remote storage. Attempt to open database on a
   * storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @since 1.4.0
   */
  public static final String LOG_ALLOW_REMOTE = "exodus.log.allowRemote";

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on RAM-disk. Attempt to open database on a storage
   * of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default value is
   * {@code false}.
   * <p>Mutable at runtime: no
   *
   * @since 1.4.0
   */
  public static final String LOG_ALLOW_RAM_DISK = "exodus.log.allowRamDisk";

  /**
   * Defines fully-qualified name of the {@linkplain DataReaderWriterProvider} service provider
   * interface implementation which will be used to create {@linkplain DataReader} and
   * {@linkplain DataWriter} instances. This setting can be used to customize storageL define
   * in-memory one, in-cloud, etc. Default value is
   * {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} which means that file
   * system must be used as a storage. Several settings are applicable only to
   * FileDataReaderWriterProvider used: {@linkplain #LOG_DURABLE_WRITE},
   * {@linkplain #LOG_SYNC_PERIOD}, {@linkplain #LOG_CACHE_OPEN_FILES},
   * {@linkplain #LOG_FULL_FILE_READ_ONLY}, {@linkplain #LOG_CACHE_USE_NIO},
   * {@linkplain #LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD}.
   * <p>Mutable at runtime: no
   */
  public static final String LOG_DATA_READER_WRITER_PROVIDER = "exodus.log.readerWriterProvider";

  /**
   * If is set to {@code true} then the {@linkplain Environment} instance is read-only. Default
   * value is {@code false}.
   * <p>Mutable at runtime: yes
   */
  public static final String ENV_IS_READONLY = "exodus.env.isReadonly";

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then the
   * {@linkplain Environment} obligatorily creates transactions for which
   * {@linkplain Transaction#isReadonly()} is {@code true}. Read-only transactions fail-fast with
   * {@linkplain ReadonlyTransactionException} on attempt to modify data. If is set to {@code false}
   * and {@linkplain #ENV_IS_READONLY} is set to {@code true} then the {@linkplain Environment}
   * creates transaction that allow to accumulate changes but cannot be flushed ot committed since
   * the {@linkplain Environment} is read-only. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @see #ENV_IS_READONLY
   */
  public static final String ENV_FAIL_FAST_IN_READONLY = "exodus.env.failFastInReadonly";

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
   * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a
   * {@linkplain Store}, but returns an empty immutable instance instead. Default value is
   * {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @see #ENV_IS_READONLY
   */
  public static final String ENV_READONLY_EMPTY_STORES = "exodus.env.readonly.emptyStores";

  /**
   * Defines the size of the "store-get" cache. The "store-get" cache can increase performance of
   * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0}
   * what means that the cache is inactive. If the setting is mutated at runtime the cache is
   * invalidated.
   * <p>Mutable at runtime: yes
   */
  public static final String ENV_STOREGET_CACHE_SIZE = "exodus.env.storeGetCacheSize";

  // TODO: document
  public static final String ENV_STOREGET_CACHE_MIN_TREE_SIZE = "exodus.env.storeGetCache.minTreeSize";

  // TODO: document
  public static final String ENV_STOREGET_CACHE_MAX_VALUE_SIZE = "exodus.env.storeGetCache.maxValueSize";

  /**
   * If is set to {@code true} then {@linkplain Environment#close()} doest't check if there are
   * unfinished transactions. Otherwise it checks and throws {@linkplain ExodusException} if there
   * are. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @see Environment#close()
   */
  public static final String ENV_CLOSE_FORCEDLY = "exodus.env.closeForcedly";

  /**
   * If is set to {@code true} then {@linkplain Environment} performs check of consistency of
   * datastructures stored in backup files. Default value is {@code false}. Mutable at runtime: no
   */
  public static final String ENV_CHECK_BACKUP_CONSISTENCY = "exodus.env.checkBackupConsistency";

  /**
   * List of stores to remove before compaction is started on environment.
   */
  public static final String ENV_STORES_TO_REMOVE_BEFORE_COMPACTION = "exodus.env.storesToRemoveBeforeCompaction";

  public static final String ENV_CHECK_DATA_STRUCTURES_CONSISTENCY = "exodus.env.checkDataStructuresConsistency";

  /**
   * Defines the number of millisecond which a {@linkplain Transaction} can try to flush without
   * attempts to upgrade (switch to an exclusive mode). Default value is {@code 2000L}.
   * <p>Mutable at runtime: yes
   *
   * @see Transaction
   * @see #ENV_TXN_REPLAY_MAX_COUNT
   * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
   */
  public static final String ENV_TXN_REPLAY_TIMEOUT = "exodus.env.txn.replayTimeout";

    /**
     * If is set to {@code true} then {@code jetbrains.exodus.entitystore.StoreTransaction#findContaining}
     * starts using tailored underlying implementation that yields twice throughput of the default one,
     * but sacrifices precision in operation with some Unicode specimen, namely Turkish dotted/dotless I.
     * Mutable at runtime: yes
     * Default: false
     */
    public static final String ENV_TXN_QUERY_OPTIMIZED_CONTAINS = "exodus.env.txn.optimizedContains";

  /**
   * Defines the number of times which a {@linkplain Transaction} can try to flush without attempts
   * to upgrade (switch to an exclusive mode). Default value is {@code 2}.
   * <p>Mutable at runtime: yes
   *
   * @see Transaction
   * @see #ENV_TXN_REPLAY_TIMEOUT
   * @see #ENV_TXN_DOWNGRADE_AFTER_FLUSH
   */
  public static final String ENV_TXN_REPLAY_MAX_COUNT = "exodus.env.txn.replayMaxCount";

  /**
   * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself
   * after {@linkplain Transaction#flush()}. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @see Transaction
   * @see #ENV_TXN_REPLAY_TIMEOUT
   * @see #ENV_TXN_REPLAY_MAX_COUNT
   */
  public static final String ENV_TXN_DOWNGRADE_AFTER_FLUSH = "exodus.env.txn.downgradeAfterFlush";

  /**
   * If is set to {@code true} then any write operation can be performed only in the thread which
   * the transaction was created in. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @see Transaction
   */
  public static final String ENV_TXN_SINGLE_THREAD_WRITES = "exodus.env.txn.singleThreadWrites";

  /**
   * If is set to {@code true} then each transaction, read/write or read-only, saves stack trace
   * when it is finished (aborted or committed). The stack trace is then reported with
   * {@code TransactionFinishedException}. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @see Transaction
   * @since 1.4.0
   */
  public static final String ENV_TXN_TRACE_FINISH = "exodus.env.txn.traceFinish";

  /**
   * Defines the number of {@linkplain Transaction transactions} that can be started in parallel. It
   * is unlimited by default.
   * <p>Mutable at runtime: no
   *
   * @see Transaction
   */
  public static final String ENV_MAX_PARALLEL_TXNS = "exodus.env.maxParallelTxns";

  /**
   * Defines the number of read-only {@linkplain Transaction transactions} that can be started in
   * parallel. By default it is unlimited.
   * <p>Mutable at runtime: no
   * <p>
   * As of 1.4.0, is deprecated.
   *
   * @see Transaction
   * @see Transaction#isReadonly()
   * @see #ENV_MAX_PARALLEL_TXNS
   */
  @Deprecated
  public static final String ENV_MAX_PARALLEL_READONLY_TXNS = "exodus.env.maxParallelReadonlyTxns";

  /**
   * Defines {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this
   * timeout then it is reported in logs as stuck along with stack trace which it was created with.
   * Default value is {@code 0} which means that no timeout for a {@linkplain Transaction} is
   * defined. In that case, no monitor of stuck transactions is started. Otherwise it is started for
   * each {@linkplain Environment}, though consuming only a single {@linkplain Thread} amongst all
   * environments created within a single class loader.
   * <p>Mutable at runtime: no
   *
   * @see Transaction
   * @see #ENV_MONITOR_TXNS_CHECK_FREQ
   */
  public static final String ENV_MONITOR_TXNS_TIMEOUT = "exodus.env.monitorTxns.timeout";

  /**
   * Defines {@linkplain Transaction} expiration timeout in milliseconds. If transaction doesn't
   * finish in this timeout then it is forced to be finished. Default value is {@code 8} hours.
   * {@code 0} value means that no expiration for a {@linkplain Transaction} is defined. In that
   * case, no monitor of stuck transactions is started. Otherwise it is started for each
   * {@linkplain Environment}, though consuming only a single {@linkplain Thread} amongst all
   * environments created within a single class loader.
   * <p>Mutable at runtime: no
   *
   * @see Transaction
   * @see #ENV_MONITOR_TXNS_CHECK_FREQ
   */
  public static final String ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT = "exodus.env.monitorTxns.expirationTimeout";

  /**
   * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts
   * and checks {@linkplain Environment}'s transactions with this frequency (period) specified in
   * milliseconds. Default value is {@code 60000}, one minute.
   * <p>Mutable at runtime: no
   *
   * @see Transaction
   * @see #ENV_MONITOR_TXNS_TIMEOUT
   */
  public static final String ENV_MONITOR_TXNS_CHECK_FREQ = "exodus.env.monitorTxns.checkFreq";

  /**
   * If is set to {@code true} then the {@linkplain Environment} gathers statistics. If
   * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX
   * managed bean. Default value is {@code true}.
   * <p>Mutable at runtime: no
   *
   * @see Environment#getStatistics()
   * @see #MANAGEMENT_ENABLED
   */
  public static final String ENV_GATHER_STATISTICS = "exodus.env.gatherStatistics";

  /**
   * If is set to {@code true} then the {@linkplain Environment} will compact itself on opening.
   * Default value is {@code false}.
   * <p>Mutable at runtime: no
   */
  public static final String ENV_COMPACT_ON_OPEN = "exodus.env.compactOnOpen";

    /**
     * Internal property. Not for public use.
     */
    public static final String ENV_COMPACT_IN_SINGLE_BATCH_ON_OPEN = "exodus.env.compactInSingleBatchOnOpen";

    /**
   * Defines the maximum size of page of B+Tree. Default value is {@code 128}.
   * <p>Mutable at runtime: yes
   */
  public static final String TREE_MAX_PAGE_SIZE = "exodus.tree.maxPageSize";

  /**
   * Defines the maximum size of page of duplicates sub-B+Tree. Default value is {@code 8}.
   * <p>Mutable at runtime: yes
   */
  public static final String TREE_DUP_MAX_PAGE_SIZE = "exodus.tree.dupMaxPageSize";

  /**
   * As of 1.0.5, is deprecated and has no effect.
   * <p>Mutable at runtime: no
   */
  @Deprecated
  public static final String TREE_NODES_CACHE_SIZE = "exodus.tree.nodesCacheSize";

  /**
   * If is set to {@code true} then the database garbage collector is enabled. Default value is
   * {@code true}. Switching GC off makes sense only for debugging and troubleshooting purposes.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_ENABLED = "exodus.gc.enabled";

  /**
   * Defines the number of milliseconds which the database garbage collector is postponed for after
   * the {@linkplain Environment} is created. Default value is {@code 10000}.
   * <p>Mutable at runtime: no
   */
  public static final String GC_START_IN = "exodus.gc.startIn";

  /**
   * Defines percent of minimum database utilization. Default value is {@code 50}. That means that
   * 50 percent of free space in raw data in {@code Log} files (.xd files) is allowed. If database
   * utilization is less than defined (free space percent is more than {@code 50}), the database
   * garbage collector is triggered.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_MIN_UTILIZATION = "exodus.gc.minUtilization";

  /**
   * If is set to {@code true} the database garbage collector renames files rather than deletes
   * them. Default value is {@code false}. It makes sense to change this setting only for debugging
   * and troubleshooting purposes.
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
   * Defines the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the
   * database garbage collector. The age of the last (the newest, the rightmost) {@code Log} file is
   * {@code 0}, the age of previous file is {@code 1}, etc. Default value is {@code 2}.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_MIN_FILE_AGE = "exodus.gc.fileMinAge";

  /**
   * Defines the number of new {@code Log} files (.xd files) that must be created to trigger if
   * necessary (if database utilization is not sufficient) the next background cleaning cycle
   * (single run of the database garbage collector) after the previous cycle finished. Default value
   * is {@code 3}, i.e. GC can start after each 3 newly created {@code Log} files.
   * <p>Mutable at runtime: yes
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static final String GC_FILES_INTERVAL = "exodus.gc.filesInterval";

  /**
   * Defines the number of milliseconds after which background cleaning cycle (single run of the
   * database garbage collector) can be repeated if the previous one didn't reach required
   * utilization. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_RUN_PERIOD = "exodus.gc.runPeriod";

  /**
   * If is set to {@code true} then database utilization will be computed from scratch before the
   * first cleaning cycle (single run of the database garbage collector) is triggered, i.e. shortly
   * after the database is open. In addition, can be used to compute utilization information at
   * runtime by just modifying the setting value. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_UTILIZATION_FROM_SCRATCH = "exodus.gc.utilization.fromScratch";

  /**
   * If is not empty, defines full path to the file with stored utilization. Is used on creation of
   * an {@linkplain Environment} to update {@code .xd} files' utilization before the first cleaning
   * cycle (single run of the database garbage collector) is triggered. In addition, can be used to
   * reload utilization information at runtime by just modifying the setting value. Format of the
   * stored utilization is expected to be the same as created by the {@code "-d"} option of the
   * {@code Reflect} tool. Default value is empty string.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_UTILIZATION_FROM_FILE = "exodus.gc.utilization.fromFile";

  /**
   * If is set to {@code true} the database garbage collector tries to acquire exclusive
   * {@linkplain Transaction} for its purposes. In that case, GC transaction never re-plays. In
   * order to not block background cleaner thread forever, acquisition of exclusive GC transaction
   * is performed with a timeout controlled by the {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT}
   * setting. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @see #GC_TRANSACTION_ACQUIRE_TIMEOUT
   * @see #GC_TRANSACTION_TIMEOUT
   * @see Transaction#isExclusive()
   */
  public static final String GC_USE_EXCLUSIVE_TRANSACTION = "exodus.gc.useExclusiveTransaction";

  /**
   * Defines timeout in milliseconds which is used by the database garbage collector to acquire
   * exclusive {@linkplain Transaction} for its purposes if
   * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}. Default value is {@code 1000}.
   * <p>Mutable at runtime: yes
   *
   * @see #GC_USE_EXCLUSIVE_TRANSACTION
   * @see #GC_TRANSACTION_TIMEOUT
   * @see Transaction#isExclusive()
   */
  public static final String GC_TRANSACTION_ACQUIRE_TIMEOUT = "exodus.gc.transactionAcquireTimeout";

  /**
   * Defines timeout in milliseconds which is used by the database garbage collector to reclaim
   * non-expired data in several files inside single GC {@linkplain Transaction} acquired
   * exclusively. {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}. Default value
   * is {@code 500}.
   * <p>Mutable at runtime: yes
   *
   * @see #GC_USE_EXCLUSIVE_TRANSACTION
   * @see #GC_TRANSACTION_ACQUIRE_TIMEOUT
   * @see Transaction#isExclusive()
   */
  public static final String GC_TRANSACTION_TIMEOUT = "exodus.gc.transactionTimeout";

  /**
   * Defines the number of milliseconds which deletion of any successfully cleaned {@code Log} file
   * (.xd file) is postponed for. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_FILES_DELETION_DELAY = "exodus.gc.filesDeletionDelay";

  /**
   * If set to nonzero value, GC is forced every this number of seconds. Default value is {@code 0}
   * which means that GC is not forced periodically.
   * <p>Mutable at runtime: yes
   */
  public static final String GC_RUN_EVERY = "exodus.gc.runEvery";

  /**
   * If is set to {@code true} then the {@linkplain Environment} exposes two JMX managed beans. One
   * for {@linkplain Environment#getStatistics() environment statistics} and second for controlling
   * the {@code EnvironmentConfig} settings. Default value is {@code true} for non-Android OS, under
   * Android it is always {@code false}.
   * <p>Mutable at runtime: no
   *
   * @see Environment#getStatistics()
   * @see Environment#getEnvironmentConfig()
   */
  public static final String MANAGEMENT_ENABLED = "exodus.managementEnabled";

  /**
   * If is set to {@code true} then exposed JMX managed beans cannot have operations. Default value
   * is {@code true}.
   * <p>Mutable at runtime: no
   *
   * @see #MANAGEMENT_ENABLED
   */
  public static final String MANAGEMENT_OPERATIONS_RESTRICTED = "exodus.management.operationsRestricted";

  /**
   * If set to some value different from {@code null}, expose created environment via given server.
   * <p>Mutable at runtime: no
   */
  public static final String META_SERVER = "exodus.env.metaServer";

  public EnvironmentConfig() {
    this(ConfigurationStrategy.SYSTEM_PROPERTY);
  }

  @SuppressWarnings("rawtypes")
  public EnvironmentConfig(@NotNull final ConfigurationStrategy strategy) {
    //noinspection unchecked
    super(new Pair[]{
        new Pair(MEMORY_USAGE_PERCENTAGE, 50),
        new Pair(USE_VERSION1_FORMAT, true),
        new Pair(CIPHER_ID, null),
        new Pair(CIPHER_KEY, null),
        new Pair(CIPHER_BASIC_IV, 0L),
        new Pair(PROFILER_ENABLED, false),
        new Pair(LOG_DURABLE_WRITE, false),
        new Pair(LOG_FILE_SIZE, 8192L),
        new Pair(LOG_LOCK_TIMEOUT, 0L),
        new Pair(LOG_LOCK_ID, null),
        new Pair(LOG_CACHE_PAGE_SIZE, 64 << 10),
        new Pair(LOG_CACHE_OPEN_FILES, 500),
        new Pair(LOG_CACHE_USE_NIO, false),
        new Pair(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, 1_000_000_000L), // ~1GB
        new Pair(LOG_CACHE_SHARED, true),
        new Pair(LOG_CACHE_NON_BLOCKING, true),
        new Pair(LOG_CACHE_GENERATION_COUNT, 2),
        new Pair(LOG_CACHE_USE_SOFT_REFERENCES, false),
        new Pair(LOG_CACHE_READ_AHEAD_MULTIPLE, 1),
        new Pair(LOG_CACHE_WARMUP, false),
        new Pair(LOG_CLEAN_DIRECTORY_EXPECTED, false),
        new Pair(LOG_CLEAR_INVALID, false),
        new Pair(LOG_SYNC_PERIOD, 10000L),
        new Pair(LOG_FULL_FILE_READ_ONLY, true),
        new Pair(LOG_ALLOW_REMOVABLE, false),
        new Pair(LOG_ALLOW_REMOTE, false),
        new Pair(LOG_ALLOW_RAM_DISK, false),
        new Pair(LOG_DATA_READER_WRITER_PROVIDER,
            DataReaderWriterProvider.DEFAULT_READER_WRITER_PROVIDER),
        new Pair(ENV_IS_READONLY, false),
        new Pair(ENV_FAIL_FAST_IN_READONLY, true),
        new Pair(ENV_READONLY_EMPTY_STORES, false),
        new Pair(ENV_STOREGET_CACHE_SIZE, 0),
        new Pair(ENV_STOREGET_CACHE_MIN_TREE_SIZE, 200),
        new Pair(ENV_STOREGET_CACHE_MAX_VALUE_SIZE, 200),
        new Pair(ENV_CLOSE_FORCEDLY, false),
        new Pair(ENV_TXN_REPLAY_TIMEOUT, 2000L),
        new Pair(ENV_TXN_QUERY_OPTIMIZED_CONTAINS, false),
        new Pair(ENV_TXN_REPLAY_MAX_COUNT, 2),
        new Pair(ENV_TXN_DOWNGRADE_AFTER_FLUSH, true),
        new Pair(ENV_TXN_SINGLE_THREAD_WRITES, false),
        new Pair(ENV_CHECK_BACKUP_CONSISTENCY, false),
                new Pair(ENV_CHECK_DATA_STRUCTURES_CONSISTENCY, false),
                new Pair(ENV_TXN_TRACE_FINISH, false),
                new Pair(ENV_MAX_PARALLEL_TXNS, Integer.MAX_VALUE),
                new Pair(ENV_MONITOR_TXNS_TIMEOUT, 0),
                new Pair(ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT, (int) TimeUnit.HOURS.toMillis(8)),
                new Pair(ENV_MONITOR_TXNS_CHECK_FREQ, 60000),
                new Pair(ENV_GATHER_STATISTICS, true),
                new Pair(ENV_COMPACT_ON_OPEN, false),
                new Pair(TREE_MAX_PAGE_SIZE, 128),
                new Pair(TREE_DUP_MAX_PAGE_SIZE, 8),
                new Pair(GC_ENABLED, true),
                new Pair(GC_START_IN, 10000),
                new Pair(GC_MIN_UTILIZATION, 50),
                new Pair(GC_RENAME_FILES, false),
                new Pair(GC_MIN_FILE_AGE, 2),
                new Pair(GC_FILES_INTERVAL, 3),
                new Pair(GC_RUN_PERIOD, 5000),
                new Pair(GC_UTILIZATION_FROM_SCRATCH, false),
                new Pair(GC_UTILIZATION_FROM_FILE, ""),
                new Pair(GC_FILES_DELETION_DELAY, 5000),
                new Pair(GC_RUN_EVERY, 0),
                new Pair(GC_USE_EXCLUSIVE_TRANSACTION, true),
                new Pair(GC_TRANSACTION_ACQUIRE_TIMEOUT, 1000),
                new Pair(GC_TRANSACTION_TIMEOUT, 500),
                new Pair(MANAGEMENT_ENABLED, !JVMConstants.getIS_ANDROID()),
                new Pair(MANAGEMENT_OPERATIONS_RESTRICTED, true),
                new Pair(META_SERVER, null),
                new Pair(CHECK_PAGES_AT_RUNTIME, true),
                new Pair(LOG_SKIP_INVALID_LOGGALE_TYPE, false),
                new Pair(LOG_FORCE_CHECK_DATA_CONSISTENCY, false),
                new Pair(LOG_PROCEED_DATA_RESTORE_AT_ANY_COST, false),
                new Pair(ENV_COMPACT_IN_SINGLE_BATCH_ON_OPEN, false),new Pair(ENV_COMPACT_IN_SINGLE_BATCH_ON_OPEN, false),
        new Pair(ENV_STORES_TO_REMOVE_BEFORE_COMPACTION, null)
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
   * Set {@code true} for making it possible to change settings of this {@code EnvironmentConfig}
   * instance. {@code EnvironmentConfig.DEFAULT} is always immutable.
   *
   * @param isMutable {@code true} if this {@code EnvironmentConfig} instance can be mutated
   * @return this {@code EnvironmentConfig} instance
   */
  @Override
  public EnvironmentConfig setMutable(boolean isMutable) {
    return (EnvironmentConfig) super.setMutable(isMutable);
  }

  /**
   * Returns absolute value of memory in bytes that can be used by the LogCache if it is set. By
   * default, is not set.
   * <p>Mutable at runtime: no
   *
   * @return absolute value of memory in bytes that can be used by the LogCache if it is set or
   * {@code null}
   * @see #getMemoryUsagePercentage()
   */
  public Long /* NB! do not change to long */ getMemoryUsage() {
    return (Long) getSetting(MEMORY_USAGE);
  }

  /**
   * Sets absolute value of memory in bytes that can be used by the LogCache. Overrides memory
   * percent to use by the LogCache set by {@linkplain #setMemoryUsagePercentage(int)}.
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
   * Returns percent of max memory (specified by the "-Xmx" java parameter) that can be used by the
   * LogCache. Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is
   * {@code 50}.
   * <p>Mutable at runtime: no
   *
   * @return percent of max memory that can be used by the LogCache
   * @see #getMemoryUsage()
   */
  public int getMemoryUsagePercentage() {
    return (Integer) getSetting(MEMORY_USAGE_PERCENTAGE);
  }

  /**
   * Sets percent of max memory (specified by the "-Xmx" java parameter) that can be used by the
   * LogCache. Is applicable only if {@linkplain #MEMORY_USAGE} is not set. Default value is
   * {@code 50}.
   * <p>Mutable at runtime: no
   *
   * @param memoryUsagePercentage percent of max memory that can be used by the LogCache
   * @return this {@code EnvironmentConfig} instance
   * @see #setMemoryUsage(long)
   */
  public EnvironmentConfig setMemoryUsagePercentage(final int memoryUsagePercentage) {
    return setSetting(MEMORY_USAGE_PERCENTAGE, memoryUsagePercentage);
  }

  public boolean getUseVersion1Format() {
    return (Boolean) getSetting(USE_VERSION1_FORMAT);
  }

  public EnvironmentConfig setUseVersion1Format(final boolean useVersion1Format) {
    return setSetting(USE_VERSION1_FORMAT, useVersion1Format);
  }

  /**
   * Indicates if disk page consistency is checked at runtime.
   *
   * @see #CHECK_PAGES_AT_RUNTIME
   */
  public boolean getCheckPagesAtRuntime() {
    return (Boolean) getSetting(CHECK_PAGES_AT_RUNTIME);
  }

  /**
   * Sets if disk page consistency is checked at runtime.
   *
   * @param checkPagesAtRuntime {@code ture} if consistency of disk pages is checked during runtime
   *                            and {@code false} otherwise.
   * @see #CHECK_PAGES_AT_RUNTIME
   */
  public EnvironmentConfig setCheckPagesAtRuntime(final boolean checkPagesAtRuntime) {
    return setSetting(CHECK_PAGES_AT_RUNTIME, checkPagesAtRuntime);
  }

  /**
   * Returns id of {@linkplain StreamCipherProvider} which will be used to encrypt the database.
   * Default value is {@code null}, which means that the database won't be encrypted. The setting
   * cannot be changed for existing databases. Default value is {@code null}.
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
   * Sets id of {@linkplain StreamCipherProvider} which will be used to encrypt the database.
   * Default value is {@code null}, which means that the database won't be encrypted. The setting
   * cannot be changed for existing databases. Default value is {@code null}.
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
   * Returns the key which will be used to encrypt the database or {@code null} for no encryption.
   * Is applicable only if* {@linkplain #getCipherId()} returns not {@code null}. Default value is
   * {@code null}.
   * <p>Mutable at runtime: no
   *
   * @return the key which will be used to encrypt the database or {@code null} for no encryption
   * @see #getCipherId()
   * @see #getCipherBasicIV()
   * @see StreamCipher
   * @see StreamCipher#init(byte[], long)
   * @see StreamCipherProvider
   */
  public byte @Nullable [] getCipherKey() {
    Object cipherKey = getSetting(CIPHER_KEY);
    if (cipherKey instanceof String) {
      cipherKey = KryptKt.toBinaryKey((String) cipherKey);
      setSetting(CIPHER_KEY, cipherKey);
    }
    return (byte[]) cipherKey;
  }

  /**
   * Sets the key which will be used to encrypt the database. The key is expected to be a hex string
   * representing a byte array which is passed to {@linkplain StreamCipher#init(byte[], long)}. Is
   * applicable only if {@linkplain #getCipherId()} returns not {@code null}. The setting cannot be
   * changed for existing databases. Default value is {@code null}.
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
   * Returns basic IV (initialization vector) which will be used to encrypt the database. Basic IV
   * is expected to be random (pseudo-random) and unique long value. Basic IV is used to calculate
   * relative IVs which are passed to {@linkplain StreamCipher#init(byte[], long)}. Is applicable
   * only if {@linkplain #CIPHER_ID} is not {@code null}. The setting cannot be changed for existing
   * databases. Default value is {@code 0L}.
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
   * Sets basic IV (initialization vector) which will be used to encrypt the database. Basic IV is
   * expected to be random (pseudo-random) and unique long value. Basic IV is used to calculate
   * relative IVs which are passed to {@linkplain StreamCipher#init(byte[], long)}. Is applicable
   * only if {@linkplain #CIPHER_ID} is not {@code null}. The setting cannot be changed for existing
   * databases. Default value is {@code 0L}.
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
   * If is set to {@code true} database profiler is enabled. By default, it is disabled.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if database profiler is enabled.
   * @since 1.4.0
   */
  public boolean getProfilerEnabled() {
    return (Boolean) getSetting(PROFILER_ENABLED);
  }

  /**
   * Set {@code true} to enable database profiler. By default, it is disabled.
   * <p>Mutable at runtime: no
   *
   * @param enabled {@code true} to enable database profiler.
   * @return this {@code EnvironmentConfig} instance
   * @since 1.4.0
   */
  public EnvironmentConfig setProfilerEnabled(final boolean enabled) {
    return setSetting(PROFILER_ENABLED, enabled);
  }

  /**
   * Returns {@code true} if file system's fsync call after should be  executed after each committed
   * or flushed transaction. By default, is switched off since it creates significant performance
   * overhead and can be controlled manually.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if {@linkplain Transaction transactions} are durable
   * @see #getLogSyncPeriod()
   */
  public boolean getLogDurableWrite() {
    return (Boolean) getSetting(LOG_DURABLE_WRITE);
  }

  /**
   * Sets flag whether {@linkplain Transaction transactions} should force fsync after each commit or
   * flush. By default, is switched off since it creates significant performance overhead and can be
   * controlled manually.
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
   * Returns the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting
   * cannot be changed for existing databases. Default value is {@code 8192L}.
   * <p>Mutable at runtime: no
   *
   * @return maximum size in kilobytes of a single .xd file
   */
  public long getLogFileSize() {
    return (Long) getSetting(LOG_FILE_SIZE);
  }

  /**
   * Sets the maximum size in kilobytes of a single {@code Log} file (.xd file). The setting cannot
   * be changed for existing databases. Default value is {@code 8192L}.
   * <p>Mutable at runtime: no
   *
   * @param kilobytes maximum size in kilobytes of a single .xd file
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogFileSize(final long kilobytes) {
    return setSetting(LOG_FILE_SIZE, kilobytes);
  }

  /**
   * Returns the number of milliseconds the {@code Log} constructor waits for the lock file. Default
   * value is {@code 0L}, i.e. it doesn't wait and fails immediately if the lock is acquired.
   * <p>Mutable at runtime: no
   *
   * @return number of milliseconds the {@code Log} constructor waits for the lock file
   */
  public long getLogLockTimeout() {
    return (Long) getSetting(LOG_LOCK_TIMEOUT);
  }

  /**
   * Sets the debug identifier to be written to the lock file alongside with other debug
   * information.
   * <p>Mutable at runtime: no
   *
   * @param id the debug identifier to be written to the lock file alongside with other debug
   *           information
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
   * Sets the debug identifier to be written to the lock file alongside with other debug
   * information. Default value is {@code ManagementFactory.getRuntimeMXBean().getName()} which has
   * a form of {@code pid@hostname}.
   * <p>Mutable at runtime: no
   *
   * @return the debug identifier to be written to the lock file alongside with other debug
   * information or null if the default value is used
   */
  public String getLogLockId() {
    return (String) getSetting(LOG_LOCK_ID);
  }

  /**
   * Returns the size in bytes of a single page (byte array) in the LogCache. This number of bytes
   * is read from {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at
   * a time.
   *
   * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s
   * should be configured
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
   * Sets the size in bytes of a single page (byte array) in the LogCache. This number of bytes is
   * read from {@linkplain java.nio.MappedByteBuffer} or {@linkplain java.io.RandomAccessFile} at a
   * time.
   *
   * <p>If the LogCache is shared ({@linkplain #LOG_CACHE_SHARED}) all {@linkplain Environment}s
   * should be configured
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
   * Returns the maximum number of open files that the LogCache maintains in order to reduce system
   * calls to open and close files. The more open files is allowed the less system calls are
   * performed in addition to reading or mapping files. This value can notably affect performance of
   * database warm-up. Default value is {@code 500}. Open files cache is shared amongst all open
   * {@linkplain Environment environments}.
   * <p>Mutable at runtime: no
   *
   * @return maximum number of open files
   */
  public int getLogCacheOpenFilesCount() {
    return (Integer) getSetting(LOG_CACHE_OPEN_FILES);
  }

  /**
   * Sets the maximum number of open files that the LogCache maintains in order to reduce system
   * calls to open and close files. The more open files is allowed the less system calls are
   * performed in addition to reading or mapping files. This value can notably affect performance of
   * database warm-up. Default value is {@code 500}. Open files cache is shared amongst all open
   * {@linkplain Environment environments}.
   * <p>Mutable at runtime: no
   *
   * @param files maximum number of open files
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheOpenFilesCount(final int files) {
    return setSetting(LOG_CACHE_OPEN_FILES, files);
  }

  /**
   * Forces to check data consistency of the database on opening. Default value is {@code false}.
   */
  public boolean getLogForceCheckDataConsistency() {
    return (Boolean) getSetting(LOG_FORCE_CHECK_DATA_CONSISTENCY);
  }

  /**
   * Forces to check data consistency of the database on opening. Default value is {@code false}.
   *
   * <p>Mutable at runtime: no
   *
   * @param checkLogDataConsistency {@code true} to check data consistency of the database on
   *                                opening
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogForceCheckDataConsistency(final boolean checkLogDataConsistency) {
    return setSetting(LOG_FORCE_CHECK_DATA_CONSISTENCY, checkLogDataConsistency);
  }

  /**
   * Forces data restore routine to proceed even if it is not possible to restore all the data.
   */
  public boolean isLogProceedDataRestoredAtAnyCost() {
    return (Boolean) getSetting(LOG_PROCEED_DATA_RESTORE_AT_ANY_COST);
  }

  /**
   * Forces data restore routine to proceed even if it is not possible to restore all the data.
   *
   * <p>Mutable at runtime: no
   */
  public EnvironmentConfig setLogProceedDataRestoredAtAnyCost(
      final boolean proceedDataRestoredAtAnyCost) {
    return setSetting(LOG_PROCEED_DATA_RESTORE_AT_ANY_COST, proceedDataRestoredAtAnyCost);
  }

  /**
   * Returns {@code true} if any immutable .xd file can be mapped in memory provided there is enough
   * physical memory. On cache miss, LogCache at first tries to check if there is corresponding
   * mapped byte buffer and copies cache page from the buffer, otherwise reads the page from
   * {@linkplain java.io.RandomAccessFile}. If is set to {@code false} then LogCache always reads
   * {@linkplain java.io.RandomAccessFile} on cache miss. Default value was {@code true} before
   * version {@code 1.2.3}. As of {@code 1.2.3}, default value is {@code false}.
   *
   * <p>Mutable at runtime: no
   *
   * @return {@code true} mapping of .xd files in memory is allowed
   * @see #getLogCacheFreePhysicalMemoryThreshold()
   * @deprecated Because of upcoming release of virtual threads feature this property is deprecated.
   */
  @Deprecated
  public boolean getLogCacheUseNio() {
    return (Boolean) getSetting(LOG_CACHE_USE_NIO);
  }

  /**
   * Set {@code true} to allow any immutable .xd file to be mapped in memory provided there is
   * enough physical memory. On cache miss, LogCache at first tries to check if there is
   * corresponding mapped byte buffer and copies cache page from the buffer, otherwise reads the
   * page from {@linkplain java.io.RandomAccessFile}. If is set to {@code false} then LogCache
   * always reads {@linkplain java.io.RandomAccessFile} on cache miss. Default value was
   * {@code true} before version {@code 1.2.3}. As of {@code 1.2.3}, default value is
   * {@code false}.
   *
   * <p>Mutable at runtime: no
   *
   * @param useNio {@code true} is using NIO is allowed
   * @return this {@code EnvironmentConfig} instance
   * @deprecated Because of upcoming release of virtual threads feature this property is deprecated.
   */
  @Deprecated
  public EnvironmentConfig setLogCacheUseNio(final boolean useNio) {
    return setSetting(LOG_CACHE_USE_NIO, useNio);
  }

  /**
   * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} defines the minimum size in bytes of free
   * physical memory maintained by the cache memory-mapped files on the host where the JVM runs. On
   * cache miss, LogCache checks if corresponding file is mapped in memory, and if it is not and if
   * free physical memory amount is greater than threshold specified by this setting, tries to map
   * the file in memory. Default value is {@code 1_000_000_000L} bytes, i.e. ~1GB.
   *
   * @return minimum size in bytes of free physical memory
   */
  public long getLogCacheFreePhysicalMemoryThreshold() {
    return (Long) getSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD);
  }

  /**
   * If {@linkplain #LOG_CACHE_USE_NIO} is {@code true} sets the minimum size in bytes of free
   * physical memory maintained by the cache memory-mapped files on the host where the JVM runs. On
   * cache miss, LogCache checks if corresponding file is mapped in memory, and if it is not and if
   * free physical memory amount is greater than threshold specified by this setting, tries to map
   * the file in memory. Default value is {@code 1_000_000_000L} bytes, i.e. ~1GB.
   *
   * @param freePhysicalMemoryThreshold minimum size in bytes of free physical memory
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheFreePhysicalMemoryThreshold(
      final long freePhysicalMemoryThreshold) {
    return setSetting(LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD, freePhysicalMemoryThreshold);
  }

  /**
   * Returns {@code true} if the LogCache is shared. Shared cache caches raw binary data (contents
   * of .xd files) of all {@linkplain Environment} instances created in scope of this class loader.
   * By default, the LogCache is shared.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the LogCache is shared
   */
  public boolean isLogCacheShared() {
    return (Boolean) getSetting(LOG_CACHE_SHARED);
  }

  /**
   * Set {@code true} if the LogCache should be shared. Shared cache caches raw binary data
   * (contents of .xd files) of all {@linkplain Environment} instances created in scope of this
   * class loader. By default, the LogCache is shared.
   * <p>Mutable at runtime: no
   *
   * @param shared {@code true} if the LogCache should be shared
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheShared(final boolean shared) {
    return setSetting(LOG_CACHE_SHARED, shared);
  }

  /**
   * Returns {@code true} if the LogCache should use lock-free data structures. Default value is
   * {@code true}. There is no practical sense to use "blocking" cache, so the setting will be
   * deprecated in future.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the LogCache should use lock-free data structures
   */
  public boolean isLogCacheNonBlocking() {
    return (Boolean) getSetting(LOG_CACHE_NON_BLOCKING);
  }

  /**
   * Set {@code true} if the LogCache should use lock-free data structures. Default value is
   * {@code true}. There is no practical sense to use "blocking" cache, so the setting will be
   * deprecated in future.
   * <p>Mutable at runtime: no
   *
   * @param nonBlocking {@code true} if the LogCache should use lock-free data structures
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheNonBlocking(final boolean nonBlocking) {
    return setSetting(LOG_CACHE_NON_BLOCKING, nonBlocking);
  }

  /**
   * Returns the number of generations of non-blocking LogCache. Is applicable only if
   * {@linkplain #LOG_CACHE_NON_BLOCKING} is set to {@code true}. The higher number of generations
   * is, the higher the cache hit rate is and CPU ticks necessary to get a single page from the
   * cache. Default value is {@code 2}.
   *
   * @return number of generations of non-blocking LogCache
   */
  public int getLogCacheGenerationCount() {
    return (Integer) getSetting(LOG_CACHE_GENERATION_COUNT);
  }

  /**
   * Sets the number of generations of non-blocking LogCache. Is applicable only if
   * {@linkplain #LOG_CACHE_NON_BLOCKING} is set to {@code true}. The higher number of generations
   * is, the higher the cache hit rate is and CPU ticks necessary to get a single page from the
   * cache. Default value is {@code 2}.
   *
   * @param generationCount number of generations of non-blocking LogCache
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheGenerationCount(final int generationCount) {
    if (generationCount < 2) {
      throw new InvalidSettingException("LogCache generation count should greater than 1");
    }
    return setSetting(LOG_CACHE_GENERATION_COUNT, generationCount);
  }

  /**
   * Returns {@code true} LogCache uses {@linkplain java.lang.ref.SoftReference soft references} for
   * holding cached pages. The cache still uses not more memory than it is configured by
   * {@linkplain #MEMORY_USAGE} or {@linkplain #MEMORY_USAGE_PERCENTAGE} settings, but JVM GC can
   * reclaim memory used by the cache on a heavy load surge. On the other hand, use of soft
   * references results in greater JVM GC load and greater general CPU consumption by the cache. So
   * one can choose either memory-flexible, or CPU-optimal cache. Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if LogCache should use
   * {@linkplain java.lang.ref.SoftReference soft references} for holding cached pages
   */
  public boolean getLogCacheUseSoftReferences() {
    return (Boolean) getSetting(LOG_CACHE_USE_SOFT_REFERENCES);
  }

  /**
   * Set {@code true} if LogCache should use
   * {@linkplain java.lang.ref.SoftReference soft references} for holding cached pages. The cache
   * still uses not more memory than it is configured by {@linkplain #MEMORY_USAGE} or
   * {@linkplain #MEMORY_USAGE_PERCENTAGE} settings, but JVM GC can reclaim memory used by the cache
   * on a heavy load surge. On the other hand, use of soft references results in greater JVM GC load
   * and greater general CPU consumption by the cache. So one can choose either memory-flexible, or
   * CPU-optimal cache. Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param useSoftReferences {@code true} if LogCache should use
   *                          {@linkplain java.lang.ref.SoftReference soft references} for holding
   *                          cached pages
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheUseSoftReferences(final boolean useSoftReferences) {
    return setSetting(LOG_CACHE_USE_SOFT_REFERENCES, useSoftReferences);
  }

  /**
   * Returns the number of successive pages to be read at once in case of LogCache miss. Reading
   * successive pages can reduce amount of random access to database files. It can be useful in
   * workloads like application warm-up. Default value is {@code 1} which means that no read-ahead
   * strategy is applied.
   *
   * @return number of successive pages to be read at once in case of LogCache miss
   */
  public int getLogCacheReadAheadMultiple() {
    return (Integer) getSetting(LOG_CACHE_READ_AHEAD_MULTIPLE);
  }

  /**
   * Sets the number of successive pages to be read at once in case of LogCache miss. Reading
   * successive pages can reduce amount of random access to database files. It can be useful in
   * workloads like application warm-up. Default value is {@code 1} which means that no read-ahead
   * strategy is applied.
   *
   * @param readAheadMultiple number of successive pages to be read at once in case of LogCache
   *                          miss
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheReadAheadMultiple(final int readAheadMultiple) {
    if (readAheadMultiple < 1) {
      throw new InvalidSettingException("LogCache read ahead multiple should greater than 0");
    }
    return setSetting(LOG_CACHE_READ_AHEAD_MULTIPLE, readAheadMultiple);
  }

  /**
   * Returns {@code true} if LogCache will populate itself with database file pages right after the
   * database is opened using this {@code EnvironmentConfig} instance. Default value is
   * {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if LogCache will populate itself with database file pages
   */
  public boolean getLogCacheWarmup() {
    return (Boolean) getSetting(LOG_CACHE_WARMUP);
  }

  /**
   * Set {@code true} if LogCache should populate itself with database file pages right after the
   * database is opened using this {@code EnvironmentConfig} instance. Default value is
   * {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param warmup {@code true} if LogCache should populate itself with database file pages
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCacheWarmup(final boolean warmup) {
    return setSetting(LOG_CACHE_WARMUP, warmup);
  }

  /**
   * Returns {@code true} if the Log constructor fails if the database directory is not clean. Can
   * be useful if an applications expects that the database should always be newly created. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the Log constructor fails if the database directory is not clean
   */
  public boolean isLogCleanDirectoryExpected() {
    return (Boolean) getSetting(LOG_CLEAN_DIRECTORY_EXPECTED);
  }

  /**
   * Set {@code true} if the Log constructor should fail if the database directory is not clean. Can
   * be useful if an applications expects that the database should always be newly created. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param logCleanDirectoryExpected {@code true} if the Log constructor should fail if the
   *                                  database directory is not clean
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogCleanDirectoryExpected(final boolean logCleanDirectoryExpected) {
    return setSetting(LOG_CLEAN_DIRECTORY_EXPECTED, logCleanDirectoryExpected);
  }

  /**
   * Returns {@code true} if the Log constructor implicitly clears the database if it occurred to be
   * invalid when opened. Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the Log constructor should implicitly clear invalid database
   */
  public boolean isLogClearInvalid() {
    return (Boolean) getSetting(LOG_CLEAR_INVALID);
  }

  /**
   * Set {@code true} if the Log constructor should implicitly clear the database if it occurred to
   * be invalid when opened. Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param logClearInvalid {@code true} if the Log constructor should implicitly clear invalid
   *                        database
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogClearInvalid(final boolean logClearInvalid) {
    return setSetting(LOG_CLEAR_INVALID, logClearInvalid);
  }

  public EnvironmentConfig setLogSkipInvalidLoggableType(final boolean skipInvalidLoggableType) {
    return setSetting(LOG_SKIP_INVALID_LOGGALE_TYPE, skipInvalidLoggableType);
  }

  public boolean isLogSkipInvalidLoggableType() {
    return (Boolean) getSetting(LOG_SKIP_INVALID_LOGGALE_TYPE);
  }

  /**
   * Returns the period in milliseconds to force file system's fsync call that often if
   * {@linkplain #LOG_DURABLE_WRITE} is switched off. Default value is {@code 10000L}.
   * <p>Mutable at runtime: yes
   *
   * @return milliseconds to force file system's fsync call that often
   * @see #getLogDurableWrite()
   */
  public long getLogSyncPeriod() {
    return (Long) getSetting(LOG_SYNC_PERIOD);
  }

  /**
   * Sets the period in milliseconds to force file system's fsync call that often if
   * {@linkplain #LOG_DURABLE_WRITE} is switched off. Default value is {@code 10000L}.
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
   * Returns {@code true} if each complete and immutable {@code Log} file (.xd file) should marked
   * with read-only attribute. Default value is {@code true}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if each complete and immutable .xd file should marked with read-only
   * attribute
   */
  public boolean isLogFullFileReadonly() {
    return (Boolean) getSetting(LOG_FULL_FILE_READ_ONLY);
  }

  /**
   * Set {@code true} if each complete and immutable {@code Log} file (.xd file) should marked with
   * read-only attribute. Default value is {@code true}.
   * <p>Mutable at runtime: no
   *
   * @param readonly {@code true} to mark each complete and immutable .xd file with read-only
   *                 attribute
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setFullFileReadonly(final boolean readonly) {
    return setSetting(LOG_FULL_FILE_READ_ONLY, readonly);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a removable storage. Attempt to open database
   * on a storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}.
   * Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the database can be opened on a removable storage
   * @since 1.4.0
   */
  public boolean isLogAllowRemovable() {
    return (Boolean) getSetting(LOG_ALLOW_REMOVABLE);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a removable storage. Attempt to open database
   * on a storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}.
   * Default value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param allow {@code true} to allow using database located on removable storage
   * @return this {@code EnvironmentConfig} instance
   * @since 1.4.0
   */
  public EnvironmentConfig setLogAllowRemovable(final boolean allow) {
    return setSetting(LOG_ALLOW_REMOVABLE, allow);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a remote storage. Attempt to open database on a
   * storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the database can be opened on a remote storage
   * @since 1.4.0
   */
  public boolean isLogAllowRemote() {
    return (Boolean) getSetting(LOG_ALLOW_REMOTE);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on a remote storage. Attempt to open database on a
   * storage of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param allow {@code true} to allow using database located on remote storage
   * @return this {@code EnvironmentConfig} instance
   * @since 1.4.0
   */
  public EnvironmentConfig setLogAllowRemote(final boolean allow) {
    return setSetting(LOG_ALLOW_REMOTE, allow);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on RAM-disk. Attempt to open database on a storage
   * of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default value is
   * {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the database can be opened on RAM-disk
   * @since 1.4.0
   */
  public boolean isLogAllowRamDisk() {
    return (Boolean) getSetting(LOG_ALLOW_RAM_DISK);
  }

  /**
   * For {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} used as
   * {@linkplain DataReaderWriterProvider} service provider interface implementation, if is set to
   * {@code true} then the database can be opened on RAM-disk. Attempt to open database on a storage
   * of not allowed type results in {@linkplain StorageTypeNotAllowedException}. Default value is
   * {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param allow {@code true} to allow using database located on RAM-disk
   * @return this {@code EnvironmentConfig} instance
   * @since 1.4.0
   */
  public EnvironmentConfig setLogAllowRamDisk(final boolean allow) {
    return setSetting(LOG_ALLOW_RAM_DISK, allow);
  }

  /**
   * Returns fully-qualified name of the {@linkplain DataReaderWriterProvider} service provide
   * interface implementation which will be used to create {@linkplain DataReader} and
   * {@linkplain DataWriter} instances. This setting can be used to customize storage: define
   * in-memory one, in-cloud, etc. Default value is
   * {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} which means that file
   * system must be used as a storage. Several settings are applicable only if
   * FileDataReaderWriterProvider is used: {@linkplain #LOG_DURABLE_WRITE},
   * {@linkplain #LOG_SYNC_PERIOD}, {@linkplain #LOG_CACHE_OPEN_FILES},
   * {@linkplain #LOG_FULL_FILE_READ_ONLY}, {@linkplain #LOG_CACHE_USE_NIO},
   * {@linkplain #LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD}.
   * <p>Mutable at runtime: no
   *
   * @return fully-qualified name of the {@linkplain DataReaderWriterProvider} service provide
   * interface implementation
   */
  public String getLogDataReaderWriterProvider() {
    return (String) getSetting(LOG_DATA_READER_WRITER_PROVIDER);
  }

  /**
   * Sets fully-qualified name of the {@linkplain DataReaderWriterProvider} service provide
   * interface implementation which will be used to create {@linkplain DataReader} and
   * {@linkplain DataWriter} instances. This setting can be used to customize storage: define
   * in-memory one, in-cloud, etc. Default value is
   * {@linkplain DataReaderWriterProvider#DEFAULT_READER_WRITER_PROVIDER} which means that file
   * system must be used as a storage. Several settings are applicable only if
   * FileDataReaderWriterProvider is used: {@linkplain #LOG_DURABLE_WRITE},
   * {@linkplain #LOG_SYNC_PERIOD}, {@linkplain #LOG_CACHE_OPEN_FILES},
   * {@linkplain #LOG_FULL_FILE_READ_ONLY}, {@linkplain #LOG_CACHE_USE_NIO},
   * {@linkplain #LOG_CACHE_FREE_PHYSICAL_MEMORY_THRESHOLD}.
   * <p>Mutable at runtime: no
   *
   * @param provider fully-qualified name of the {@linkplain DataReaderWriterProvider} service
   *                 provide interface implementation
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setLogDataReaderWriterProvider(@NotNull final String provider) {
    return setSetting(LOG_DATA_READER_WRITER_PROVIDER, provider);
  }

  /**
   * Returns {@code true} if the {@linkplain Environment} instance is read-only. Default value is
   * {@code false}.
   * <p>Mutable at runtime: yes
   * <p>
   * *WARNING* do not use this method to check if {@linkplain Environment} is working in read-only
   * mode at runtime. Please use {@linkplain  Environment#isReadOnly()} instead.
   *
   * @return {@code true} if the {@linkplain Environment} instance is read-only
   */
  public boolean getEnvIsReadonly() {
    return (Boolean) getSetting(ENV_IS_READONLY);
  }

  /**
   * Set {@code true} to turn the {@linkplain Environment} instance to read-only mode. Default value
   * is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @param isReadonly {@code true} to turn the {@linkplain Environment} instance to read-only mode
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvIsReadonly(final boolean isReadonly) {
    return setSetting(ENV_IS_READONLY, isReadonly);
  }

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then the
   * {@linkplain Environment} obligatorily creates transactions for which
   * {@linkplain Transaction#isReadonly()} is {@code true}. Read-only transactions fail-fast with
   * {@linkplain ReadonlyTransactionException} on attempt to modify data. If is set to {@code false}
   * and {@linkplain #ENV_IS_READONLY} is set to {@code true} then the {@linkplain Environment}
   * creates transaction that allow to accumulate changes but cannot be flushed ot committed since
   * the {@linkplain Environment} is read-only. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if attempt to modify data won't fail immediately in read-only mode
   */
  public boolean getEnvFailFastInReadonly() {
    return (Boolean) getSetting(ENV_FAIL_FAST_IN_READONLY);
  }

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then the
   * {@linkplain Environment} obligatorily creates transactions for which
   * {@linkplain Transaction#isReadonly()} is {@code true}. Read-only transactions fail-fast with
   * {@linkplain ReadonlyTransactionException} on attempt to modify data. If is set to {@code false}
   * and {@linkplain #ENV_IS_READONLY} is set to {@code true} then the {@linkplain Environment}
   * creates transaction that allow to accumulate changes but cannot be flushed ot committed since
   * the {@linkplain Environment} is read-only. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @param failFast {@code true} if attempt modify data shouldn fail immediately in read-only mode
   * @return this {@code EnvironmentConfig} instance
   */
  @SuppressWarnings("UnusedReturnValue")
  public EnvironmentConfig setEnvFailFastInReadonly(final boolean failFast) {
    return setSetting(ENV_FAIL_FAST_IN_READONLY, failFast);
  }

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
   * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a
   * {@linkplain Store}, but returns an empty immutable instance instead. Default value is
   * {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if attempt to create a {@linkplain Store} won't fail immediately in
   * read-only mode
   */
  public boolean getEnvReadonlyEmptyStores() {
    return (Boolean) getSetting(ENV_READONLY_EMPTY_STORES);
  }

  /**
   * If is set to {@code true} and {@linkplain #ENV_IS_READONLY} is also {@code true} then
   * {@linkplain Environment#openStore(String, StoreConfig, Transaction)} doesn't try to create a
   * {@linkplain Store}, but returns an empty immutable instance instead. Default value is
   * {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @param readonlyEmptyStores {@code true} if attempt to create a {@linkplain Store} shouldn't
   *                            fail immediately in read-only mode
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvReadonlyEmptyStores(final boolean readonlyEmptyStores) {
    return setSetting(ENV_READONLY_EMPTY_STORES, readonlyEmptyStores);
  }

  /**
   * Returns the size of the "store-get" cache. The "store-get" cache can increase performance of
   * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0}
   * what means that the cache is inactive. If the setting is mutated at runtime the cache is
   * invalidated.
   * <p>Mutable at runtime: yes
   *
   * @return size of the "store-get" cache
   */
  public int getEnvStoreGetCacheSize() {
    return (Integer) getSetting(ENV_STOREGET_CACHE_SIZE);
  }

  /**
   * Sets the size of the "store-get" cache. The "store-get" cache can increase performance of
   * {@linkplain Store#get(Transaction, ByteIterable)} in certain cases. Default value is {@code 0}
   * what means that the cache is inactive. If the setting is mutated at runtime the cache is
   * invalidated.
   * <p>Mutable at runtime: yes
   *
   * @param storeGetCacheSize size of the "store-get" cache
   * @return this {@code EnvironmentConfig} instance
   */
  @SuppressWarnings("UnusedReturnValue")
  public EnvironmentConfig setEnvStoreGetCacheSize(final int storeGetCacheSize) {
    if (storeGetCacheSize < 0) {
      throw new InvalidSettingException("Negative StoreGetCache size");
    }
    return setSetting(ENV_STOREGET_CACHE_SIZE, storeGetCacheSize);
  }

  public int getEnvStoreGetCacheMinTreeSize() {
    return (Integer) getSetting(ENV_STOREGET_CACHE_MIN_TREE_SIZE);
  }

  public EnvironmentConfig setEnvStoreGetCacheMinTreeSize(final int treeSize) {
    if (treeSize < 0) {
      throw new InvalidSettingException("Negative tree size");
    }
    return setSetting(ENV_STOREGET_CACHE_MIN_TREE_SIZE, treeSize);
  }

  public int getEnvStoreGetCacheMaxValueSize() {
    return (Integer) getSetting(ENV_STOREGET_CACHE_MAX_VALUE_SIZE);
  }

  public EnvironmentConfig setEnvStoreGetCacheMaxValueSize(final int valueSize) {
    if (valueSize < 0) {
      throw new InvalidSettingException("Negative value size");
    }
    return setSetting(ENV_STOREGET_CACHE_MAX_VALUE_SIZE, valueSize);
  }

  /**
   * Returns {@code true} if {@linkplain Environment#close()} shouldn't check if there are
   * unfinished transactions. Otherwise it should check and throw {@linkplain ExodusException} if
   * there are. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if {@linkplain Environment#close()} shouldn't check unfinished
   * transactions
   * @see Environment#close()
   */
  public boolean getEnvCloseForcedly() {
    return (Boolean) getSetting(ENV_CLOSE_FORCEDLY);
  }

  /**
   * List of stores to remove before compaction is started on environment.
   */
  public String getStoresToRemoveBeforeCompaction() {
    return (String) getSetting(ENV_STORES_TO_REMOVE_BEFORE_COMPACTION);
  }

  /**
   * Set {@code true} if {@linkplain Environment#close()} shouldn't check if there are unfinished
   * transactions. Set {@code false} if it should check and throw {@linkplain ExodusException} if
   * there are unfinished transactions. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @param closeForcedly {@code true} if {@linkplain Environment#close()} should ignore unfinished
   *                      transactions
   * @return this {@code EnvironmentConfig} instance
   * @see Environment#close()
   */
  public EnvironmentConfig setEnvCloseForcedly(final boolean closeForcedly) {
    return setSetting(ENV_CLOSE_FORCEDLY, closeForcedly);
  }

  /**
   * If is set to {@code true} then {@linkplain Environment} performs check of consistency of
   * datastructures stored in backup files.
    * Default value is {@code false}.
   * <p>Mutable at runtime: no
   */
  public boolean getCheckBackupConsistency() {
    return (Boolean) getSetting(ENV_CHECK_BACKUP_CONSISTENCY);
  }

  public boolean getCheckDataStructuresConsistency() {
    return (Boolean) getSetting(ENV_CHECK_DATA_STRUCTURES_CONSISTENCY);
  }


  /**
   * Returns the number of millisecond which a {@linkplain Transaction} can try to flush without
   * attempts to upgrade (switch to an exclusive mode). Default value is {@code 2000L}.
   * <p>Mutable at runtime: yes
   *
   * @return number of millisecond which a {@linkplain Transaction} can try to flush without
   * attempts to upgrade
   * @see Transaction
   * @see #getEnvTxnReplayMaxCount()
   */
  public long getEnvTxnReplayTimeout() {
    return (Long) getSetting(ENV_TXN_REPLAY_TIMEOUT);
  }

  /**
   * Sets the number of millisecond which a {@linkplain Transaction} can try to flush without
   * attempts to upgrade (switch to an exclusive mode). Default value is {@code 2000L}.
   * <p>Mutable at runtime: yes
   *
   * @param timeout number of millisecond which a {@linkplain Transaction} can try to flush without
   *                attempts to upgrade
   * @return this {@code EnvironmentConfig} instanceR
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
   * Returns the number of times which a {@linkplain Transaction} can try to flush without attempts
   * to upgrade (switch to an exclusive mode). Default value is {@code 2}.
   * <p>Mutable at runtime: yes
   *
   * @return number of times which a {@linkplain Transaction} can try to flush without attempts to
   * upgrade
   * @see Transaction
   * @see #getEnvTxnReplayTimeout()
   */
  public int getEnvTxnReplayMaxCount() {
    return (Integer) getSetting(ENV_TXN_REPLAY_MAX_COUNT);
  }

  /**
   * Returns whether to use optimized contains implementation for `propertyContains` request,
   * that yields higher throughput, but fails on some Unicode specimen.
   *
   * @return whether to use optimized contains
   * @see ENV_TXN_QUERY_OPTIMIZED_CONTAINS
   * @see jetbrains.exodus.entitystore.StoreTransaction#findContaining(String, String, String, boolean)
   */
  public boolean getEnvQueryOptimizedContains() {
      return (Boolean) getSetting(ENV_TXN_QUERY_OPTIMIZED_CONTAINS);
  }

  /**
   * Sets {@link ENV_TXN_QUERY_OPTIMIZED_CONTAINS}, see its description
   */
  public EnvironmentConfig setEnvQueryOptimizedContains(final boolean value) {
      return setSetting(ENV_TXN_QUERY_OPTIMIZED_CONTAINS, value);
  }

  /**
   * Sets the number of times which a {@linkplain Transaction} can try to flush without attempts to
   * upgrade (switch to an exclusive mode). Default value is {@code 2}.
   * <p>Mutable at runtime: yes
   *
   * @param count number of times which a {@linkplain Transaction} can try to flush without attempts
   *              to upgrade
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
   * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself
   * after {@linkplain Transaction#flush()}. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if any upgraded {@linkplain Transaction} will downgrade itself after flush
   */
  public boolean getEnvTxnDowngradeAfterFlush() {
    return (Boolean) getSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH);
  }

  /**
   * If is set to {@code true} then any upgraded {@linkplain Transaction} will downgrade itself
   * after {@linkplain Transaction#flush()}. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @param downgrade {@code true} if any upgraded {@linkplain Transaction} will downgrade itself
   *                  after flush
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvTxnDowngradeAfterFlush(final boolean downgrade) {
    return setSetting(ENV_TXN_DOWNGRADE_AFTER_FLUSH, downgrade);
  }

  /**
   * If is set to {@code true} then any write operation can be performed only in the thread which
   * the transaction was created in. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if any write operation can be performed only in the thread which the
   * transaction was created in
   * @see Transaction
   */
  public boolean getEnvTxnSingleThreadWrites() {
    return (Boolean) getSetting(ENV_TXN_SINGLE_THREAD_WRITES);
  }

  /**
   * If is set to {@code true} then any write operation can be performed only in the thread which
   * the transaction was created in. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @param singleThreadWrites {@code true} then any write operation can be performed only in the
   *                           thread which the transaction was created in
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvTxnSingleThreadWrites(final boolean singleThreadWrites) {
    return setSetting(ENV_TXN_SINGLE_THREAD_WRITES, singleThreadWrites);
  }

  /**
   * If is set to {@code true} then each transaction, read/write or read-only, saves stack trace
   * when it is finished (aborted or committed). The stack trace is then reported with
   * {@code TransactionFinishedException}. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if each transaction saves stack trace when it is finished
   * @see Transaction
   * @since 1.4.0
   */
  public boolean isEnvTxnTraceFinish() {
    return (Boolean) getSetting(ENV_TXN_TRACE_FINISH);
  }

  /**
   * If is set to {@code true} then each transaction, read/write or read-only, saves stack trace
   * when it is finished (aborted or committed). The stack trace is then reported with
   * {@code TransactionFinishedException}. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @param traceFinish {@code true} if each transaction should save stack trace when it is
   *                    finished
   * @return this {@code EnvironmentConfig} instance
   * @see Transaction
   * @since 1.4.0
   */
  public EnvironmentConfig setEnvTxnTraceFinish(final boolean traceFinish) {
    return setSetting(ENV_TXN_TRACE_FINISH, traceFinish);
  }

  /**
   * Returns the number of {@linkplain Transaction transactions} that can be started in parallel. By
   * default it is unlimited.
   * <p>Mutable at runtime: no
   *
   * @return number of {@linkplain Transaction transactions} that can be started in parallel
   */
  public int getEnvMaxParallelTxns() {
    return (Integer) getSetting(ENV_MAX_PARALLEL_TXNS);
  }

  /**
   * Sets the number of {@linkplain Transaction transactions} that can be started in parallel. By
   * default it is unlimited.
   * <p>Mutable at runtime: no
   *
   * @param maxParallelTxns number of {@linkplain Transaction transactions} that can be started in
   *                        parallel
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvMaxParallelTxns(final int maxParallelTxns) {
    return setSetting(ENV_MAX_PARALLEL_TXNS, maxParallelTxns);
  }

  /**
   * Returns the number of read-only {@linkplain Transaction transactions} that can be started in
   * parallel. By default it is unlimited.
   * <p>Mutable at runtime: no
   * <p>
   * As of 1.4.0, is deprecated.
   *
   * @return number of read-only {@linkplain Transaction transactions} that can be started in
   * parallel
   */
  @SuppressWarnings("MethodMayBeStatic")
  @Deprecated
  public int getEnvMaxParallelReadonlyTxns() {
    return Integer.MAX_VALUE;
  }

  /**
   * Sets the number of read-only {@linkplain Transaction transactions} that can be started in
   * parallel. By default it is unlimited.
   * <p>Mutable at runtime: no
   * <p>
   * As of 1.4.0, is deprecated.
   *
   * @param maxParallelReadonlyTxns number of read-only {@linkplain Transaction transactions} that
   *                                can be started in parallel
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvMaxParallelReadonlyTxns(final int maxParallelReadonlyTxns) {
    return this;
  }

  /**
   * Returns {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this
   * timeout then it is reported in logs as stuck along with stack trace which it was created with.
   * Default value is {@code 0} which means that no timeout for a {@linkplain Transaction} is
   * defined. In that case, no monitor of stuck transactions is started. Otherwise it is started for
   * each {@linkplain Environment}, though consuming only a single {@linkplain Thread} amongst all
   * environments created within a single class loader.
   * <p>Mutable at runtime: no
   *
   * @return timeout of a {@linkplain Transaction} in milliseconds
   * @see #getEnvMonitorTxnsCheckFreq()
   */
  public int getEnvMonitorTxnsTimeout() {
    return (Integer) getSetting(ENV_MONITOR_TXNS_TIMEOUT);
  }

  /**
   * Sets {@linkplain Transaction} timeout in milliseconds. If transaction doesn't finish in this
   * timeout then it is reported in logs as stuck along with stack trace which it was created with.
   * Default value is {@code 0} which means that no timeout for a {@linkplain Transaction} is
   * defined. In that case, no monitor of stuck transactions is started. Otherwise it is started for
   * each {@linkplain Environment}, though consuming only a single {@linkplain Thread} amongst all
   * environments created within a single class loader.
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
   * Defines {@linkplain Transaction} expiration timeout in milliseconds. If transaction doesn't
   * finish in this timeout then it is forced to be finished. Default value is {@code 8} hours.
   * {@code 0} value means that no expiration for a {@linkplain Transaction} is defined. Otherwise
   * it is started for each {@linkplain Environment}, though consuming only a single
   * {@linkplain Thread} amongst all environments created within a single class loader.
   * <p>Mutable at runtime: no
   *
   * @return expiration timeout of a {@linkplain Transaction} in milliseconds
   * @see #getEnvMonitorTxnsCheckFreq()
   */
  public int getEnvMonitorTxnsExpirationTimeout() {
    return (Integer) getSetting(ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT);
  }

  /**
   * Defines {@linkplain Transaction} expiration timeout in milliseconds. If transaction doesn't
   * finish in this timeout then it is forced to be finished. Default value is {@code 8} hours.
   * {@code 0} value means that no expiration for a {@linkplain Transaction} is defined. Otherwise
   * it is started for each {@linkplain Environment}, though consuming only a single
   * {@linkplain Thread} amongst all environments created within a single class loader.
   * <p>Mutable at runtime: no
   *
   * @param timeout timeout of a {@linkplain Transaction} in milliseconds
   * @return this {@code EnvironmentConfig} instance
   * @see #setEnvMonitorTxnsCheckFreq(int)
   */
  public EnvironmentConfig setEnvMonitorTxnsExpirationTimeout(final int timeout) {
    if (timeout != 0 && timeout < 1000) {
      throw new InvalidSettingException("Transaction timeout should be greater than a second");
    }
    setSetting(ENV_MONITOR_TXNS_EXPIRATION_TIMEOUT, timeout);
    if (timeout > 0 && timeout < getEnvMonitorTxnsCheckFreq()) {
      setEnvMonitorTxnsCheckFreq(timeout);
    }
    return this;
  }

  /**
   * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts
   * and checks {@linkplain Environment}'s transactions with this frequency (period) specified in
   * milliseconds. Default value is {@code 60000}, one minute.
   * <p>Mutable at runtime: no
   *
   * @return frequency (period) in milliseconds of checking stuck transactions
   * @see #getEnvMonitorTxnsTimeout()
   */
  public int getEnvMonitorTxnsCheckFreq() {
    return (Integer) getSetting(ENV_MONITOR_TXNS_CHECK_FREQ);
  }

  /**
   * If {@linkplain #ENV_MONITOR_TXNS_TIMEOUT} is non-zero then stuck transactions monitor starts
   * and checks {@linkplain Environment}'s transactions with this frequency (period) specified in
   * milliseconds. Default value is {@code 60000}, one minute.
   * <p>Mutable at runtime: no
   *
   * @param freq frequency (period) in milliseconds of checking stuck transactions
   * @return this {@code EnvironmentConfig} instance
   * @see #setEnvMonitorTxnsTimeout(int)
   * @see #setEnvMonitorTxnsExpirationTimeout(int)
   */
  public EnvironmentConfig setEnvMonitorTxnsCheckFreq(final int freq) {
    return setSetting(ENV_MONITOR_TXNS_CHECK_FREQ, freq);
  }

  /**
   * Returns {@code true} if the {@linkplain Environment} gathers statistics. If
   * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX
   * managed bean. Default value is {@code true}.
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
   * {@linkplain #MANAGEMENT_ENABLED} is also {@code true} then the statistics is exposed by the JMX
   * managed bean. Default value is {@code true}.
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
   * Returns {@code true} if the {@linkplain Environment} will compact itself on opening. Default
   * value is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if the {@linkplain Environment} will compact itself on opening
   */
  public boolean getEnvCompactOnOpen() {
    return (Boolean) getSetting(ENV_COMPACT_ON_OPEN);
  }

  /**
   * Set {@code true} if the {@linkplain Environment} should compact itself on opening Default value
   * is {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param compactOnOpen {@code true} if the {@linkplain Environment} should  compact itself on
   *                      opening
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setEnvCompactOnOpen(final boolean compactOnOpen) {
    return setSetting(ENV_COMPACT_ON_OPEN, compactOnOpen);
  }

    /**
     * Internal property. Not for public use.
     */
    public boolean getEnvCompactInSingleBatchOnOpen() {
        return (Boolean) getSetting(ENV_COMPACT_IN_SINGLE_BATCH_ON_OPEN);
    }

    /**
     * Internal property. Not for public use.
     */
    public EnvironmentConfig setEnvCompactInSingleBatchOnOpen(final boolean compactInSingleBatchOnOpen) {
        return setSetting(ENV_COMPACT_IN_SINGLE_BATCH_ON_OPEN, compactInSingleBatchOnOpen);
    }


    /**
   * Returns the maximum size of page of B+Tree. Default value is {@code 128}.
   * <p>Mutable at runtime: yes
   *
   * @return maximum size of page of B+Tree
   */
  public int getTreeMaxPageSize() {
    return (Integer) getSetting(TREE_MAX_PAGE_SIZE);
  }

  /**
   * Sets the maximum size of page of B+Tree. Default value is {@code 128}. Only sizes in the range
   * [16..1024] are accepted.
   * <p>Mutable at runtime: yes
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
   * Returns the maximum size of page of duplicates sub-B+Tree. Default value is {@code 8}.
   * <p>Mutable at runtime: yes
   *
   * @return maximum size of page of duplicates sub-B+Tree
   */
  public int getTreeDupMaxPageSize() {
    return (Integer) getSetting(TREE_DUP_MAX_PAGE_SIZE);
  }

  /**
   * Sets the maximum size of page of duplicates sub-B+Tree. Default value is {@code 8}. Only sizes
   * in the range [8..128] are accepted.
   * <p>Mutable at runtime: yes
   *
   * @param pageSize maximum size of page of duplicates sub-B+Tree
   * @return this {@code EnvironmentConfig} instance
   * @throws InvalidSettingException page size is not in the range [8..128]
   */
  public EnvironmentConfig setTreeDupMaxPageSize(final int pageSize)
      throws InvalidSettingException {
    if (pageSize < 8 || pageSize > 128) {
      throw new InvalidSettingException("Invalid dup tree page size: " + pageSize);
    }
    return setSetting(TREE_DUP_MAX_PAGE_SIZE, pageSize);
  }

  /**
   * As of 1.0.5, is deprecated and has no effect.
   * <p>Mutable at runtime: no
   *
   * @return {@code 0}
   */
  @SuppressWarnings("MethodMayBeStatic")
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
   * Returns {@code true} if the database garbage collector is enabled. Default value is
   * {@code true}. Switching GC off makes sense only for debugging and troubleshooting purposes.
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
   * Returns the number of milliseconds which the database garbage collector is postponed for after
   * the {@linkplain Environment} is created. Default value is {@code 10000}.
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
   * @param startInMillis number of milliseconds which the database garbage collector should be
   *                      postponed for after the {@linkplain Environment} is created
   * @return this {@code EnvironmentConfig} instance
   * @throws InvalidSettingException {@code startInMillis} is negative
   */
  @SuppressWarnings("UnusedReturnValue")
  public EnvironmentConfig setGcStartIn(final int startInMillis) throws InvalidSettingException {
    if (startInMillis < 0) {
      throw new InvalidSettingException(
          "GC can't be postponed for that number of milliseconds: " + startInMillis);
    }
    return setSetting(GC_START_IN, startInMillis);
  }

  /**
   * Returns percent of minimum database utilization. Default value is {@code 50}. That means that
   * 50 percent of free space in raw data in {@code Log} files (.xd files) is allowed. If database
   * utilization is less than defined (free space percent is more than {@code 50}), the database
   * garbage collector is triggered.
   * <p>Mutable at runtime: yes
   *
   * @return percent of minimum database utilization
   */
  public int getGcMinUtilization() {
    return (Integer) getSetting(GC_MIN_UTILIZATION);
  }

  /**
   * Sets percent of minimum database utilization. Default value is {@code 50}. That means that 50
   * percent of free space in raw data in {@code Log} files (.xd files) is allowed. If database
   * utilization is less than defined (free space percent is more than {@code 50}), the database
   * garbage collector is triggered.
   * <p>Mutable at runtime: yes
   *
   * @param percent percent of minimum database utilization
   * @return this {@code EnvironmentConfig} instance
   * @throws InvalidSettingException {@code percent} is not in the range [1..90]
   */
  @SuppressWarnings("UnusedReturnValue")
  public EnvironmentConfig setGcMinUtilization(int percent) throws InvalidSettingException {
    if (percent < 1 || percent > 90) {
      throw new InvalidSettingException("Invalid minimum log files utilization: " + percent);
    }
    return setSetting(GC_MIN_UTILIZATION, percent);
  }

  /**
   * If returns {@code true} the database garbage collector renames files rather than deletes them.
   * Default value is {@code false}. It makes sense to change this setting only for debugging and
   * troubleshooting purposes.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if the database garbage collector should rename files rather than deletes
   * them
   */
  public boolean getGcRenameFiles() {
    return (Boolean) getSetting(GC_RENAME_FILES);
  }

  /**
   * Set {@code true} if the database garbage collector should rename files rather than deletes
   * them. Default value is {@code false}. It makes sense to change this setting only for debugging
   * and troubleshooting purposes.
   * <p>Mutable at runtime: yes
   *
   * @param rename {@code true} if the database garbage collector should rename files rather than
   *               deletes them
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
  @SuppressWarnings("MethodMayBeStatic")
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
   * Returns the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the
   * database garbage collector. The age of the last (the newest, the rightmost) {@code Log} file is
   * {@code 0}, the age of previous file is {@code 1}, etc. Default value is {@code 2}.
   * <p>Mutable at runtime: yes
   *
   * @return minimum age of .xd file to consider it for cleaning by the database garbage collector
   */
  public int getGcFileMinAge() {
    return (Integer) getSetting(GC_MIN_FILE_AGE);
  }

  /**
   * Returns the minimum age of a {@code Log} file (.xd file) to consider it for cleaning by the
   * database garbage collector. The age of the last (the newest, the rightmost) {@code Log} file is
   * {@code 0}, the age of previous file is {@code 1}, etc. Default value is {@code 2}. The age
   * cannot be less than {@code 1}.
   * <p>Mutable at runtime: yes
   *
   * @param minAge minimum age of .xd file to consider it for cleaning by the database garbage
   *               collector
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
   * Returns the number of new {@code Log} files (.xd files) that must be created to trigger if
   * necessary (if database utilization is not sufficient) the next background cleaning cycle
   * (single run of the database garbage collector) after the previous cycle finished. Default value
   * is {@code 3}, i.e. GC can start after each 3 newly created {@code Log} files.
   * <p>Mutable at runtime: yes
   *
   * @return number of new .xd files that must be created to trigger the next background cleaning
   * cycle
   */
  @Deprecated
  public int getGcFilesInterval() {
    return (Integer) getSetting(GC_FILES_INTERVAL);
  }

  /**
   * Sets the number of new {@code Log} files (.xd files) that must be created to trigger if
   * necessary (if database utilization is not sufficient) the next background cleaning cycle
   * (single run of the database garbage collector) after the previous cycle finished. Default value
   * is {@code 3}, i.e. GC can start after each 3 newly created {@code Log} files. Cannot be less
   * than {@code 1}.
   * <p>Mutable at runtime: yes
   *
   * @param files number of new .xd files that must be created to trigger the next background
   *              cleaning cycle
   * @return this {@code EnvironmentConfig} instance
   * @throws InvalidSettingException {@code files} is less than {@code 1}
   */
  @Deprecated
  public EnvironmentConfig setGcFilesInterval(final int files) throws InvalidSettingException {
    if (files < 1) {
      throw new InvalidSettingException("Invalid number of files: " + files);
    }
    return setSetting(GC_FILES_INTERVAL, files);
  }

  /**
   * Returns the number of milliseconds after which background cleaning cycle (single run of the
   * database garbage collector) can be repeated if the previous one didn't reach required
   * utilization. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   *
   * @return number of milliseconds after which background cleaning cycle can be repeated if the
   * previous one didn't reach required utilization
   */
  public int getGcRunPeriod() {
    return (Integer) getSetting(GC_RUN_PERIOD);
  }

  /**
   * Sets the number of milliseconds after which background cleaning cycle (single run of the
   * database garbage collector) can be repeated if the previous one didn't reach required
   * utilization. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   *
   * @param runPeriod number of milliseconds after which background cleaning cycle can be repeated
   *                  if the previous one didn't reach required utilization
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
   * Returns {@code true} if database utilization will be computed from scratch before the first
   * cleaning cycle (single run of the database garbage collector) is triggered, i.e. shortly after
   * the database is open. In addition, can be used to compute utilization information at runtime by
   * just modifying the setting value. Default value is {@code false}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if database utilization will be computed from scratch
   */
  public boolean getGcUtilizationFromScratch() {
    return (Boolean) getSetting(GC_UTILIZATION_FROM_SCRATCH);
  }

  /**
   * Set {@code true} if database utilization should be computed from scratch before the first
   * cleaning cycle (single run of the database garbage collector) is triggered, i.e. shortly after
   * the database is open. In addition, can be used to compute utilization information at runtime by
   * just modifying the setting value. Default value is {@code false}.
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
   * cycle (single run of the database garbage collector) is triggered. In addition, can be used to
   * reload utilization information at runtime by just modifying the setting value. Format of the
   * stored utilization is expected to be the same as created by the {@code "-d"} option of the
   * {@code Reflect} tool. Default value is empty string.
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
   * cycle (single run of the database garbage collector) is triggered. In addition, can be used to
   * reload utilization information at runtime by just modifying the setting value. Format of the
   * stored utilization is expected to be the same as created by the {@code "-d"} option of the
   * {@code Reflect} tool. Default value is empty string.
   * <p>Mutable at runtime: yes
   *
   * @param file full path to the file with stored utilization
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setGcUtilizationFromFile(final String file) {
    return setSetting(GC_UTILIZATION_FROM_FILE, file);
  }

  /**
   * Returns {@code true} if the database garbage collector tries to acquire exclusive
   * {@linkplain Transaction} for its purposes. In that case, GC transaction never re-plays. In
   * order to not block background cleaner thread forever, acquisition of exclusive GC transaction
   * is performed with a timeout controlled by the {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT}
   * setting. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @return {@code true} if the database garbage collector tries to acquire exclusive
   * {@linkplain Transaction}
   * @see #getGcTransactionAcquireTimeout()
   * @see #getGcTransactionTimeout()
   */
  public boolean getGcUseExclusiveTransaction() {
    return (Boolean) getSetting(GC_USE_EXCLUSIVE_TRANSACTION);
  }

  /**
   * Sets {@code true} if the database garbage collector should try to acquire exclusive
   * {@linkplain Transaction} for its purposes. In that case, GC transaction never re-plays. In
   * order to not block background cleaner thread forever, acquisition of exclusive GC transaction
   * is performed with a timeout controlled by the {@linkplain #GC_TRANSACTION_ACQUIRE_TIMEOUT}
   * setting. Default value is {@code true}.
   * <p>Mutable at runtime: yes
   *
   * @param useExclusiveTransaction {@code true} if the database garbage collector should try to
   *                                acquire exclusive {@linkplain Transaction}
   * @return this {@code EnvironmentConfig} instance
   * @see #setGcTransactionAcquireTimeout(int)
   * @see #setGcTransactionTimeout(int)
   */
  public EnvironmentConfig setGcUseExclusiveTransaction(final boolean useExclusiveTransaction) {
    return setSetting(GC_USE_EXCLUSIVE_TRANSACTION, useExclusiveTransaction);
  }

  /**
   * Returns timeout in milliseconds which is used by the database garbage collector to acquire
   * exclusive {@linkplain Transaction} for its purposes if
   * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}. Default value is {@code 1000}.
   * <p>Mutable at runtime: yes
   *
   * @return timeout in milliseconds which is used by the database garbage collector to acquire
   * exclusive {@linkplain Transaction}
   * @see #getGcUseExclusiveTransaction()
   * @see #getGcTransactionTimeout()
   */
  public int getGcTransactionAcquireTimeout() {
    return (Integer) getSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT);
  }

  /**
   * Sets timeout in milliseconds which is used by the database garbage collector to acquire
   * exclusive {@linkplain Transaction} for its purposes if
   * {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} is {@code true}. Default value is {@code 1000}.
   * <p>Mutable at runtime: yes
   *
   * @param txnAcquireTimeout timeout in milliseconds which is used by the database garbage
   *                          collector to acquire exclusive {@linkplain Transaction}
   * @return this {@code EnvironmentConfig} instance
   * @see #setGcUseExclusiveTransaction(boolean)
   * @see #setGcTransactionTimeout(int)
   */
  public EnvironmentConfig setGcTransactionAcquireTimeout(final int txnAcquireTimeout) {
    return setSetting(GC_TRANSACTION_ACQUIRE_TIMEOUT, txnAcquireTimeout);
  }

  /**
   * Returns timeout in milliseconds which is used by the database garbage collector to reclaim
   * non-expired data in several files inside single GC {@linkplain Transaction} acquired
   * exclusively. {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}. Default value
   * is {@code 500}.
   * <p>Mutable at runtime: yes
   *
   * @return timeout in milliseconds which is used by the database garbage collector to reclaim
   * non-expired data in several files inside single {@linkplain Transaction} acquired exclusively
   * @see #getGcUseExclusiveTransaction()
   * @see #getGcTransactionAcquireTimeout()
   */
  public int getGcTransactionTimeout() {
    return (Integer) getSetting(GC_TRANSACTION_TIMEOUT);
  }

  /**
   * Sets timeout in milliseconds which is used by the database garbage collector to reclaim
   * non-expired data in several files inside single GC {@linkplain Transaction} acquired
   * exclusively. {@linkplain #GC_USE_EXCLUSIVE_TRANSACTION} should be {@code true}. Default value
   * is {@code 500}.
   * <p>Mutable at runtime: yes
   *
   * @param txnTimeout timeout in milliseconds which is used by the database garbage collector to
   *                   reclaim non-expired data in several files inside single
   *                   {@linkplain Transaction} acquired exclusively
   * @return this {@code EnvironmentConfig} instance
   * @see #setGcUseExclusiveTransaction(boolean)
   * @see #setGcTransactionAcquireTimeout(int)
   */
  public EnvironmentConfig setGcTransactionTimeout(final int txnTimeout) {
    return setSetting(GC_TRANSACTION_TIMEOUT, txnTimeout);
  }

  /**
   * Returns the number of milliseconds which deletion of any successfully cleaned {@code Log} file
   * (.xd file) is postponed for. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   *
   * @return number of milliseconds which deletion of any successfully cleaned .xd file is postponed
   * for
   */
  public int getGcFilesDeletionDelay() {
    return (Integer) getSetting(GC_FILES_DELETION_DELAY);
  }

  /**
   * Sets the number of milliseconds which deletion of any successfully cleaned {@code Log} file
   * (.xd file) is postponed for. Default value is {@code 5000}.
   * <p>Mutable at runtime: yes
   *
   * @param delay number of milliseconds which deletion of any successfully cleaned .xd file is
   *              postponed for
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
   * GC is forced every this number of seconds. Default value is {@code 0} which means that GC is
   * not forced periodically.
   * <p>Mutable at runtime: yes
   */
  public int getGcRunEvery() {
    return (Integer) getSetting(GC_RUN_EVERY);
  }

  /**
   * Sets GC to be forced every this number of seconds. If the value is zero GC is not forced
   * periodically. Default value is {@code 0}.
   * <p>Mutable at runtime: yes
   */
  public EnvironmentConfig setGcRunEvery(final int seconds) {
    if (seconds < 0) {
      throw new InvalidSettingException("Number of seconds must be non-negative: " + seconds);
    }
    return setSetting(GC_RUN_EVERY, seconds);
  }

  /**
   * Return {@code true} if the {@linkplain Environment} exposes two JMX managed beans. One for
   * {@linkplain Environment#getStatistics() environment statistics} and second for controlling the
   * {@code EnvironmentConfig} settings. Default value is {@code true} for non-Android OS, under
   * Android it is always {@code false}.
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
   * {@code EnvironmentConfig} settings. Default value is {@code true} for non-Android OS, under
   * Android it is always {@code false}.
   * <p>Mutable at runtime: no
   *
   * @param managementEnabled {@code true} if the {@linkplain Environment} should expose JMX managed
   *                          beans
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setManagementEnabled(final boolean managementEnabled) {
    return setSetting(MANAGEMENT_ENABLED, managementEnabled && !JVMConstants.getIS_ANDROID());
  }

  /**
   * If is set to {@code true} then exposed JMX managed beans cannot have operations. Default value
   * is {@code true}.
   * <p>Mutable at runtime: no
   *
   * @return {@code true} if exposed JMX managed beans cannot have operations.
   * @see #isManagementEnabled()
   */
  public boolean getManagementOperationsRestricted() {
    return (Boolean) getSetting(MANAGEMENT_OPERATIONS_RESTRICTED);
  }

  /**
   * If {@linkplain #isManagementEnabled()} then set {@code false} in order to expose operations
   * with JMX managed beans in addition to attributes.
   *
   * @param operationsRestricted {@code false} if JMX managed beans should expose operations in
   *                             addition to attributes
   * @return this {@code EnvironmentConfig} instance
   */
  public EnvironmentConfig setManagementOperationsRestricted(final boolean operationsRestricted) {
    return setSetting(MANAGEMENT_OPERATIONS_RESTRICTED, operationsRestricted);
  }

  public EnvironmentConfig setMetaServer(final MetaServer metaServer) {
    return setSetting(META_SERVER, metaServer);
  }

  public MetaServer getMetaServer() {
    return (MetaServer) getSetting(META_SERVER);
  }
}
