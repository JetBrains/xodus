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
package jetbrains.exodus.lucene;

import jetbrains.exodus.env.ContextualEnvironment;
import jetbrains.exodus.env.EnvironmentTestsBase;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.log.LogConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class ExodusLuceneTestsBase extends EnvironmentTestsBase {


    static final String SUMMARY = "Summary";
    static final String DESCRIPTION = "Description";
    static final String SUMMARY_TEXT = "Sukhoi Superjet 100";
    static final String DESCRIPTION_TEXT =
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
            "“saberlets” debuted flight tests on 21 December 2017. They should improve hot and high airport " +
            "performance and cut costs up to $70,000 per year. Parts of the wing are reinforced for the aerodynamic " +
            "loads distribution change. " +
            "settings java.lang.NullPointerException " +
            "as a player.";

    Directory directory;
    Analyzer analyzer;
    IndexWriterConfig indexConfig;
    IndexWriter indexWriter;
    IndexReader indexReader;
    IndexSearcher indexSearcher;
    MoreLikeThis moreLikeThis;
    Transaction txn;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        beginTransaction();
        //directory = new ExodusDirectory(((ContextualEnvironment) getEnvironment()), getContentsConfig());
        //directory = new RAMDirectory();
        directory = new DebugExodusDirectory((ContextualEnvironment) getEnvironment(), getContentsConfig());
        createAnalyzer();
        createIndexWriter();
    }

    @Override
    protected void createEnvironment() {
        env = newContextualEnvironmentInstance(LogConfig.create(reader, writer));
    }

    protected abstract StoreConfig getContentsConfig();

    @After
    @Override
    public void tearDown() throws Exception {
        closeIndexReader();
        closeIndexWriter();
        commitTransaction();
        directory.close();
        super.tearDown();
    }

    protected void beginTransaction() {
        if (txn != null) {
            throw new IllegalStateException("Not committed transaction");
        }
        txn = env.beginTransaction();
    }

    protected void commitTransaction() {
        if (txn == null) {
            throw new IllegalStateException("Not started transaction");
        }
        txn.commit();
        txn = null;
    }

    protected void createAnalyzer() {
        analyzer = new EnglishAnalyzer();
    }

    protected void removeStopWord(final String stopWord) {
        final CharArraySet sourceStopWords = ((StopwordAnalyzerBase) analyzer).getStopwordSet();
        final CharArraySet stopSet = new CharArraySet(sourceStopWords.size() - 1, false);
        for (Object word : sourceStopWords) {
            if (!stopWord.equals(new String((char[]) word))) {
                stopSet.add(word);
            }
        }
        analyzer = new EnglishAnalyzer(stopSet);
    }

    protected void createIndexWriterConfig() {
        indexConfig = new IndexWriterConfig(analyzer);
        indexConfig.setMergeScheduler(new SerialMergeScheduler());
    }

    protected void createIndexWriter() throws IOException {
        closeIndexWriter();
        createIndexWriterConfig();
        indexWriter = new IndexWriter(directory, indexConfig);
    }

    protected void createIndexReader() throws IOException {
        closeIndexReader();
        indexReader = DirectoryReader.open(directory);
    }

    protected void createIndexSearcher() throws IOException {
        createIndexReader();
        indexSearcher = new IndexSearcher(indexReader);
    }

    protected void createMoreLikeThis() throws IOException {
        closeMoreLikeThis();
        createIndexSearcher();
        moreLikeThis = new MoreLikeThis(indexReader);
        moreLikeThis.setAnalyzer(analyzer);
        moreLikeThis.setMinTermFreq(1);
        moreLikeThis.setMinDocFreq(1);
    }

    protected void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
            indexWriter = null;
        }
    }

    protected void closeIndexReader() throws IOException {
        if (indexReader != null) {
            indexReader.close();
            indexReader = null;
        }
    }

    protected void closeMoreLikeThis() {
        moreLikeThis = null;
    }
}
