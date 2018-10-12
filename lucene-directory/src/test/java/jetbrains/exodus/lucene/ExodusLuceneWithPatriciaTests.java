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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class ExodusLuceneWithPatriciaTests extends ExodusLuceneTestsBase {

    private static int docId = 0;

    private static final FieldType ID_FIELD_TYPE;
    private static final FieldType TEXT_FIELD_TYPE;

    static {
        ID_FIELD_TYPE = new FieldType();
        ID_FIELD_TYPE.setTokenized(false);
        ID_FIELD_TYPE.setStored(true);
        ID_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        TEXT_FIELD_TYPE = new FieldType();
        TEXT_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        TEXT_FIELD_TYPE.setStoreTermVectors(true);
    }

    @Test
    public void addDocument() throws IOException {
        addSingleDocument();
    }

    @Test
    public void addDocumentAfterClose() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createIndexWriter();
        addSingleDocument();
    }

    @Test
    public void addSearchMatch() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createIndexSearcher();
        final TopDocs docs = indexSearcher.search(getQuery(SUMMARY, "sukhoi"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void addSearchNonExistent() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createIndexSearcher();
        final TopDocs docs = indexSearcher.search(getQuery(SUMMARY, "sukhoii"), Integer.MAX_VALUE);
        Assert.assertEquals(0, docs.totalHits);
    }

    @Test
    public void addSearchAnalyzed() throws IOException {
        addSearchMatch();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "market"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "develop"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "setting"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "settings"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void addSearchWildcards() throws IOException {
        addSearchMatch();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "mark*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "develo*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "?roject"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "*roject"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "??oject"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "*rojec?"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "?roj*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "*roje*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "*ukhoi"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void addSearchPhrase() throws IOException {
        addSearchMatch();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "\"Long-term Cooperation Agreement\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"could now start selling\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"On 10 October 2003\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"[8]\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"on the plane\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"development of the\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void addSearchExactMatch() throws IOException {
        addSearchMatch();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "\"aftersales\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"2005-2009\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"2000.[4]\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\".[6] The\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\".[8]\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void addSearchPhraseTestQuery() throws IOException {
        addSearchMatch();
        Query query = getQuery(DESCRIPTION, "\"could now start selling\"");
        Assert.assertTrue(query instanceof PhraseQuery);
        TopDocs docs = indexSearcher.search(query, Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        query = getQuery(DESCRIPTION, "\"the fourth stage\"");
        Assert.assertTrue(query instanceof PhraseQuery);
        docs = indexSearcher.search(query, Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        removeStopWord("on");
        query = getQuery(DESCRIPTION, "\"on the plane\"");
        Assert.assertTrue(query instanceof PhraseQuery);
        docs = indexSearcher.search(query, Integer.MAX_VALUE);
        Assert.assertEquals(0, docs.totalHits);
        removeStopWord("as");
        query = getQuery(DESCRIPTION, "\"as a player\"");
        Assert.assertTrue(query instanceof PhraseQuery);
        docs = indexSearcher.search(query, Integer.MAX_VALUE);
        Assert.assertEquals(0, docs.totalHits);
    }

    @Test
    public void searchExceptions() throws IOException {
        addSearchMatch();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "java.lang.NullPointerException"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "\"java.lang.NullPointerException\""), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "java*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "*java*"), Integer.MAX_VALUE);
        Assert.assertEquals(1, docs.totalHits);
    }

    @Test
    public void multipleDocuments() throws IOException {
        for (int i = 0; i < 100; ++i) {
            createIndexWriter();
            addSingleDocument();
        }
        closeIndexWriter();
        createIndexSearcher();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "market"), Integer.MAX_VALUE);
        Assert.assertEquals(100, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "develop"), Integer.MAX_VALUE);
        Assert.assertEquals(100, docs.totalHits);
    }

    @Test
    public void multipleDocuments2() throws IOException {
        for (int i = 0; i < 500; ++i) {
            if (i % 23 == 0) {
                createIndexWriter();
            }
            addSingleDocument();
        }
        closeIndexWriter();
        createIndexSearcher();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "market"), Integer.MAX_VALUE);
        Assert.assertEquals(500, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "develop"), Integer.MAX_VALUE);
        Assert.assertEquals(500, docs.totalHits);
    }

    @Test
    public void multipleDocuments3() throws IOException {
        for (int i = 0; i < 5000; ++i) {
            if (i % 500 == 0) {
                createIndexWriter();
            }
            addSingleDocument();
        }
        closeIndexWriter();
        createIndexSearcher();
        TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "market"), Integer.MAX_VALUE);
        Assert.assertEquals(5000, docs.totalHits);
        docs = indexSearcher.search(getQuery(DESCRIPTION, "develop"), Integer.MAX_VALUE);
        Assert.assertEquals(5000, docs.totalHits);
    }

    @Test
    public void multipleDocuments4() throws IOException, InterruptedException {
        multipleDocuments3();
        txn.flush();
        createIndexSearcher();
        final boolean[] wereExceptions = {false};
        final Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    env.executeInReadonlyTransaction(new TransactionalExecutable() {
                        @Override
                        public void execute(@NotNull Transaction txn) {
                            try {
                                for (int i = 0; i < 500; ++i) {
                                    TopDocs docs = indexSearcher.search(getQuery(DESCRIPTION, "market"), Integer.MAX_VALUE);
                                    Assert.assertEquals(5000, docs.totalHits);
                                    docs = indexSearcher.search(getQuery(DESCRIPTION, "develop"), Integer.MAX_VALUE);
                                    Assert.assertEquals(5000, docs.totalHits);
                                }
                            } catch (Throwable t) {
                                System.out.println(t);
                                wereExceptions[0] = true;
                            }
                        }
                    });
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertFalse(wereExceptions[0]);
    }

    @Test
    public void trivialMoreLikeThis() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createMoreLikeThis();
        moreLikeThis.setFieldNames(new String[]{DESCRIPTION});
        assertMoreLikeThis(DESCRIPTION_TEXT, DESCRIPTION, 1);
    }

    @Test
    public void simpleMoreLikeThis() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createMoreLikeThis();
        moreLikeThis.setFieldNames(new String[]{DESCRIPTION});
        assertMoreLikeThis("cooperation august sukhoi board aircraft", DESCRIPTION, 1);
    }

    @Test
    public void moreLikeThisByParticularField() throws IOException {
        addSingleDocument();
        closeIndexWriter();
        createMoreLikeThis();
        moreLikeThis.setFieldNames(new String[]{DESCRIPTION, SUMMARY});
        assertMoreLikeThis("sukhoi superjet 100", DESCRIPTION, 1);
        assertMoreLikeThis("sukhoi superjet 100", SUMMARY, 1);
        assertMoreLikeThis("cooperation august sukhoi board aircraft", DESCRIPTION, 1);
        assertMoreLikeThis("cooperation august sukhoi board aircraft", SUMMARY, 1);
        assertMoreLikeThis("ничего по-русски в индексе нет", DESCRIPTION, 0);
        assertMoreLikeThis("ничего по-русски в индексе нет", SUMMARY, 0);
    }

    private void assertMoreLikeThis(final String text, final String field, int result) throws IOException {
        final Query like = moreLikeThis.like(field, new StringReader(text));
        final TopDocs docs = indexSearcher.search(like, Integer.MAX_VALUE);
        Assert.assertEquals(result, docs.totalHits);
    }

    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    private void addSingleDocument() throws IOException {
        final int id = docId++;
        final String idValue = Integer.toString(id);
        indexWriter.deleteDocuments(new Term("doc_id", idValue));
        final Document doc = new Document();
        doc.add(new Field("doc_id", idValue, ID_FIELD_TYPE));
        doc.add(new Field(SUMMARY, SUMMARY_TEXT, TEXT_FIELD_TYPE));
        doc.add(new Field(DESCRIPTION, DESCRIPTION_TEXT, TEXT_FIELD_TYPE));
        indexWriter.addDocument(doc);
    }

    private Query getQuery(final String field, final String query) {
        final QueryParser queryParser = new QueryParser(field, analyzer);
        queryParser.setAllowLeadingWildcard(true);
        queryParser.setDefaultOperator(QueryParser.Operator.AND);
        try {
            return queryParser.parse(query);
        } catch (ParseException e) {
            throw new ExodusException(e);
        }
    }

    @Override
    protected StoreConfig getContentsConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING;
    }
}
