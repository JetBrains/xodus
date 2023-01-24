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
package jetbrains.exodus.lucene

import jetbrains.exodus.ExodusException
import jetbrains.exodus.env.StoreConfig
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.FieldType
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.ParseException
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.Query
import org.apache.lucene.store.IOContext
import org.junit.Assert
import org.junit.Test
import java.io.StringReader

open class ExodusLuceneWithPatriciaTests : ExodusLuceneTestsBase() {

    override val contentsConfig: StoreConfig
        get() = StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING

    @Test
    fun addDocument() {
        addSingleDocument()
    }

    @Test
    fun addDocumentAfterClose() {
        addSingleDocument()
        closeIndexWriter()
        createIndexWriter()
        addSingleDocument()
    }

    @Test
    fun addSearchMatch() {
        addSingleDocument()
        closeIndexWriter()
        createIndexSearcher()
        val docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.SUMMARY, "sukhoi"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun addSearchNonExistent() {
        addSingleDocument()
        closeIndexWriter()
        createIndexSearcher()
        val docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.SUMMARY, "sukhoii"), Integer.MAX_VALUE)
        Assert.assertEquals(0, docs.totalHits.value)
    }

    @Test
    fun addSearchAnalyzed() {
        addSearchMatch()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "market"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develop"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "setting"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "settings"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun addSearchWildcards() {
        addSearchMatch()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "mark*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develo*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "?roject"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "*roject"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "??oject"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "*rojec?"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "?roj*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "*roje*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "*ukhoi"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun addSearchPhrase() {
        addSearchMatch()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"Long-term Cooperation Agreement\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"could now start selling\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"On 10 October 2003\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"[8]\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"on the plane\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"development of the\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun addSearchExactMatch() {
        addSearchMatch()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"aftersales\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"2005-2009\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"2000.[4]\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\".[6] The\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\".[8]\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun addSearchPhraseTestQuery() {
        addSearchMatch()
        var query = getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"could now start selling\"")
        Assert.assertTrue(query is PhraseQuery)
        var docs = indexSearcher.search(query, Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        query = getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"the fourth stage\"")
        Assert.assertTrue(query is PhraseQuery)
        docs = indexSearcher.search(query, Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        removeStopWord("on")
        query = getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"on the plane\"")
        Assert.assertTrue(query is PhraseQuery)
        docs = indexSearcher.search(query, Integer.MAX_VALUE)
        Assert.assertEquals(0, docs.totalHits.value)
        removeStopWord("as")
        query = getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"as a player\"")
        Assert.assertTrue(query is PhraseQuery)
        docs = indexSearcher.search(query, Integer.MAX_VALUE)
        Assert.assertEquals(0, docs.totalHits.value)
    }

    @Test
    fun searchExceptions() {
        addSearchMatch()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "java.lang.NullPointerException"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "\"java.lang.NullPointerException\""), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "java*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "*java*"), Integer.MAX_VALUE)
        Assert.assertEquals(1, docs.totalHits.value)
    }

    @Test
    fun multipleDocuments() {
        for (i in 0..99) {
            createIndexWriter()
            addSingleDocument()
        }
        closeIndexWriter()
        createIndexSearcher()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "market"), Integer.MAX_VALUE)
        Assert.assertEquals(100, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develop"), Integer.MAX_VALUE)
        Assert.assertEquals(100, docs.totalHits.value)
    }

    @Test
    fun multipleDocuments2() {
        for (i in 0..499) {
            if (i % 23 == 0) {
                createIndexWriter()
            }
            addSingleDocument()
        }
        closeIndexWriter()
        createIndexSearcher()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "market"), Integer.MAX_VALUE)
        Assert.assertEquals(500, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develop"), Integer.MAX_VALUE)
        Assert.assertEquals(500, docs.totalHits.value)
    }

    @Test
    fun multipleDocuments3() {
        for (i in 0..4999) {
            if (i % 500 == 0) {
                createIndexWriter()
            }
            addSingleDocument()
        }
        closeIndexWriter()
        createIndexSearcher()
        var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "market"), Integer.MAX_VALUE)
        Assert.assertEquals(5000, docs.totalHits.value)
        docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develop"), Integer.MAX_VALUE)
        Assert.assertEquals(5000, docs.totalHits.value)
    }

    @Test
    fun multipleDocuments4() {
        multipleDocuments3()
        createIndexSearcher()
        var wereExceptions = false
        val threads = arrayOfNulls<Thread>(10)
        for (i in threads.indices) {
            threads[i] = Thread(Runnable {
                env.executeInReadonlyTransaction {
                    try {
                        for (j in 0..499) {
                            var docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "market"), Integer.MAX_VALUE)
                            Assert.assertEquals(5000, docs.totalHits.value)
                            docs = indexSearcher.search(getQuery(ExodusLuceneTestsBase.DESCRIPTION, "develop"), Integer.MAX_VALUE)
                            Assert.assertEquals(5000, docs.totalHits.value)
                            docs.scoreDocs.forEach {
                                val doc = indexSearcher.doc(it.doc, setOf("doc_id"))
                                Integer.parseInt(doc.get("doc_id"))
                            }
                        }
                    } catch (t: Throwable) {
                        println(t)
                        wereExceptions = true
                    }
                }
            })
        }
        for (thread in threads) {
            thread?.start()
        }
        for (thread in threads) {
            thread?.join()
        }
        Assert.assertFalse(wereExceptions)
    }

    @Test
    fun trivialMoreLikeThis() {
        addSingleDocument()
        closeIndexWriter()
        createMoreLikeThis()
        moreLikeThis?.fieldNames = arrayOf(ExodusLuceneTestsBase.DESCRIPTION)
        assertMoreLikeThis(ExodusLuceneTestsBase.DESCRIPTION_TEXT, ExodusLuceneTestsBase.DESCRIPTION, 1)
    }

    @Test
    fun simpleMoreLikeThis() {
        addSingleDocument()
        closeIndexWriter()
        createMoreLikeThis()
        moreLikeThis?.fieldNames = arrayOf(ExodusLuceneTestsBase.DESCRIPTION)
        assertMoreLikeThis("cooperation august sukhoi board aircraft", ExodusLuceneTestsBase.DESCRIPTION, 1)
    }

    @Test
    fun moreLikeThisByParticularField() {
        addSingleDocument()
        closeIndexWriter()
        createMoreLikeThis()
        moreLikeThis?.fieldNames = arrayOf(ExodusLuceneTestsBase.DESCRIPTION, ExodusLuceneTestsBase.SUMMARY)
        assertMoreLikeThis("sukhoi superjet 100", ExodusLuceneTestsBase.DESCRIPTION, 1)
        assertMoreLikeThis("sukhoi superjet 100", ExodusLuceneTestsBase.SUMMARY, 1)
        assertMoreLikeThis("cooperation august sukhoi board aircraft", ExodusLuceneTestsBase.DESCRIPTION, 1)
        assertMoreLikeThis("cooperation august sukhoi board aircraft", ExodusLuceneTestsBase.SUMMARY, 1)
        assertMoreLikeThis("ничего по-ру�?�?ки в индек�?е нет", ExodusLuceneTestsBase.DESCRIPTION, 0)
        assertMoreLikeThis("ничего по-ру�?�?ки в индек�?е нет", ExodusLuceneTestsBase.SUMMARY, 0)
    }

    @Test
    fun clonedFilePointer() {
        addSingleDocument()
        closeIndexWriter()
        txn?.flush()
        val nonEmptyFile = directory.listAll().first { directory.fileLength(it) > 0 }
        val input = directory.openInput(nonEmptyFile, IOContext.DEFAULT)
        Assert.assertEquals(0L, input.filePointer)
        Assert.assertEquals(0L, input.clone().filePointer)
        input.readByte()
        Assert.assertEquals(1L, input.filePointer)
        Assert.assertEquals(1L, input.clone().filePointer)
    }

    @Test
    fun clonedSliceFilePointer() {
        addSingleDocument()
        closeIndexWriter()
        txn?.flush()
        val nonEmptyFile = directory.listAll().first { directory.fileLength(it) > 0 }
        var input = directory.openInput(nonEmptyFile, IOContext.DEFAULT)
        Assert.assertEquals(0L, input.filePointer)
        Assert.assertEquals(0L, input.clone().filePointer)
        input = input.slice("", 0L, 1L)
        input.readByte()
        Assert.assertEquals(1L, input.filePointer)
        Assert.assertEquals(1L, input.clone().filePointer)
        Assert.assertEquals(0L, input.slice("", 0L, 1L).filePointer)
    }

    private fun assertMoreLikeThis(text: String, field: String, result: Int) {
        val like = moreLikeThis?.like(field, StringReader(text))
        val docs = indexSearcher.search(like, Integer.MAX_VALUE)
        Assert.assertEquals(result.toLong(), docs.totalHits.value)
    }

    private fun addSingleDocument() {
        val id = docId++
        val idValue = id.toString()
        indexWriter?.deleteDocuments(Term("doc_id", idValue))
        val doc = Document()
        doc.add(Field("doc_id", idValue, ID_FIELD_TYPE))
        doc.add(Field(SUMMARY, SUMMARY_TEXT, TEXT_FIELD_TYPE))
        doc.add(Field(DESCRIPTION, DESCRIPTION_TEXT, TEXT_FIELD_TYPE))
        indexWriter?.addDocument(doc)
    }

    private fun getQuery(field: String, query: String): Query {
        val queryParser = QueryParser(field, analyzer)
        queryParser.allowLeadingWildcard = true
        queryParser.defaultOperator = QueryParser.Operator.AND
        try {
            return queryParser.parse(query)
        } catch (e: ParseException) {
            throw ExodusException(e)
        }

    }

    companion object {

        private var docId = 0

        private val ID_FIELD_TYPE = FieldType()
        private val TEXT_FIELD_TYPE = FieldType()

        init {
            ID_FIELD_TYPE.setTokenized(false)
            ID_FIELD_TYPE.setStored(true)
            ID_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS)
            TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
        }
    }
}
