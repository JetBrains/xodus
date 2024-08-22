/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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
package jetbrains.exodus.lucene

import jetbrains.exodus.env.ContextualEnvironment
import jetbrains.exodus.env.EnvironmentTestsBase
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.lucene.codecs.Lucene87CodecWithNoFieldCompression
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.CharArraySet
import org.apache.lucene.analysis.StopwordAnalyzerBase
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.index.*
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.junit.After
import org.junit.Before

abstract class ExodusLuceneTestsBase : EnvironmentTestsBase() {

    internal lateinit var directory: Directory
    internal lateinit var analyzer: Analyzer
    internal lateinit var indexConfig: IndexWriterConfig
    internal var indexWriter: IndexWriter? = null
    internal var indexReader: IndexReader? = null
    internal lateinit var indexSearcher: IndexSearcher
    internal var moreLikeThis: MoreLikeThis? = null
    internal var txn: Transaction? = null

    protected abstract val contentsConfig: StoreConfig

    private val directoryConfig: ExodusDirectoryConfig
        get() = ExodusDirectoryConfig().apply {
            inputBufferSize = 256
            inputMergeBufferSize = 1024
        }

    @Before
    @Throws(Exception::class)
    override fun setUp() {
        super.setUp()
        beginTransaction()
        //directory = new ExodusDirectory(((ContextualEnvironment) getEnvironment()), getContentsConfig());
        //directory = new RAMDirectory();
        directory = DebugExodusDirectory(environment as ContextualEnvironment, contentsConfig, directoryConfig)
        createAnalyzer()
        createIndexWriter()
    }

    override fun createEnvironment() {
        env = newContextualEnvironmentInstance(LogConfig.create(reader, writer))
    }

    @After
    @Throws(Exception::class)
    override fun tearDown() {
        closeIndexReader()
        closeIndexWriter()
        commitTransaction()
        directory.close()
        super.tearDown()
    }

    protected fun beginTransaction() {
        if (txn != null) {
            throw IllegalStateException("Not committed transaction")
        }
        txn = env.beginTransaction()
    }

    protected fun commitTransaction() {
        (txn ?: throw IllegalStateException("Not started transaction")).apply {
            if (isReadonly) {
                abort()
            } else {
                commit()
            }
            txn = null
        }
    }

    protected fun flushTransaction() {
        (txn ?: throw IllegalStateException("Not started transaction")).apply {
            if (isReadonly) {
                abort()
            } else {
                commit()
            }
            txn = env.beginReadonlyTransaction()
        }
    }

    protected fun createAnalyzer() {
        analyzer = EnglishAnalyzer()
    }

    protected fun removeStopWord(stopWord: String) {
        val sourceStopWords = (analyzer as StopwordAnalyzerBase).stopwordSet
        val stopSet = CharArraySet(sourceStopWords.size - 1, false)
        for (word in sourceStopWords) {
            if (stopWord != String(word as CharArray)) {
                stopSet.add(word)
            }
        }
        analyzer = EnglishAnalyzer(stopSet)
    }

    protected fun createIndexWriterConfig() {
        indexConfig = IndexWriterConfig(analyzer)
        indexConfig.mergeScheduler = SerialMergeScheduler()
        indexConfig.codec = Lucene87CodecWithNoFieldCompression()
    }

    protected fun createIndexWriter() {
        closeIndexWriter()
        createIndexWriterConfig()
        indexWriter = IndexWriter(directory, indexConfig)
    }

    protected fun createIndexReader(): IndexReader {
        closeIndexReader()
        return DirectoryReader.open(directory).apply { indexReader = this }
    }

    protected fun createIndexSearcher() {
        flushTransaction()
        indexSearcher = IndexSearcher(createIndexReader())
    }

    protected fun createMoreLikeThis() {
        closeMoreLikeThis()
        createIndexSearcher()
        moreLikeThis = MoreLikeThis(indexReader).also {
            it.analyzer = analyzer
            it.minTermFreq = 1
            it.minDocFreq = 1
        }
    }

    protected fun closeIndexWriter() {
        indexWriter?.apply {
            close()
            indexWriter = null
        }
    }

    protected fun closeIndexReader() {
        indexReader?.apply {
            close()
            indexReader = null
        }
    }

    protected fun closeMoreLikeThis() {
        moreLikeThis = null
    }

    companion object {


        internal const val SUMMARY = "Summary"
        internal const val DESCRIPTION = "Description"
        internal const val SUMMARY_TEXT = "Sukhoi Superjet 100"
        internal const val DESCRIPTION_TEXT =
                "Development of the Sukhoi Superjet 100 began in 2000.[4] On 19 December 2002, Sukhoi Civil Aircraft " +
                        "and the American company Boeing signed a Long-term Cooperation Agreement to work together on the plane. " +
                        "Boeing consultants had already been advising Sukhoi on marketing, design, certification, manufacturing, " +
                        "program management and aftersales support for a year.[5] On 10 October 2003, the technical board of the " +
                        "project selected the suppliers of major subsystems.[6] The project officially passed its third stage of " +
                        "development on 12 March 2004, meaning that Sukhoi could now start selling the plane to customers.[7] On " +
                        "13 November 2004, the Superjet 100 passed the fourth stage of development, implying that the plane was " +
                        "now ready for commencing of prototype production.[8] In August 2005, a contract between the Russian " +
                        "government and Sukhoi was signed. Under the agreement, the Superjet 100 project would receive 7.9 " +
                        "billion rubles of research and development financing under the Federal Program titled Development of " +
                        "Civil Aviation in Russia in 2005-2009. " +
                        "London City Airport is a major destination for Irish CityJet, which is receiving 15 SSJ100s, but its " +
                        "steep 5.5° approach requires new control laws, wing flap setting and modified brakes: test flights will " +
                        "begin in December 2017, certification is planned for 2018, and the modified aircraft will be available " +
                        "in 2019.[58]\n" +
                        "A new \"sabrelet\" winglet, helping takeoff and landing performance and delivering 3% better fuel burn, " +
                        "will be standard and available for retrofit.[58] Designed with CFD tools by Sukhoi and TsAGI, the " +
                        "“saberlets�? debuted flight tests on 21 December 2017. They should improve hot and high airport " +
                        "performance and cut costs up to $70,000 per year. Parts of the wing are reinforced for the aerodynamic " +
                        "loads distribution change. " +
                        "settings java.lang.NullPointerException " +
                        "as a player."
    }
}
