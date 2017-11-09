/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.ComparableGetter;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityStoreTestBase;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.junit.Assert;

import static jetbrains.exodus.query.metadata.AssociationEndCardinality._0_1;
import static jetbrains.exodus.query.metadata.MetaBuilder.*;

@SuppressWarnings({"HardcodedLineSeparator", "EqualsBetweenInconvertibleTypes", "OverlyCoupledMethod"})
public class QueryTreeTest extends EntityStoreTestBase {
    private PropertyEqual propertyEqual;
    private PropertyEqual propertyEqualNull;
    private PropertyNotNull propertyNotNull;
    private LinkEqual linkEqualNull;
    private LinkEqual linkEqual;
    private LinkNotNull linkNotNull;
    private Concat concat;
    private QueryEngine queryEngine;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        queryEngine = new QueryEngine(model(
                clazz("TstClass").
                        prop("s", "string").
                        prop("i", "int").
                        link("itself", "TstClass", _0_1).
                        edge("self1", "TstClass", _0_1, "self2", _0_1).
                        link("myEnum", "MyEnum", _0_1),
                enumeration("MyEnum").
                        prop("number", "int")
        ), getEntityStore());
        SortEngine sortEngine = new SortEngine();
        queryEngine.setSortEngine(sortEngine);
        sortEngine.setQueryEngine(queryEngine);
        prepare();
    }

    public void testAnd() throws Exception {
        And and = new And(propertyEqual, linkNotNull);
        Assert.assertEquals(1, QueryUtil.getSize(instantiate(and)));
        Assert.assertFalse(and.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(and));
    }

    public void testConcat() throws Exception {
        Assert.assertEquals(3, QueryUtil.getSize(queryEngine.queryGetAll("TstClass")));
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(concat)));
        Assert.assertFalse(concat.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(concat));

        SortByProperty left = new SortByProperty(propertyEqual, "s", true);
        SortByLinkProperty right = new SortByLinkProperty(linkNotNull, "MyEnum", "number", "itself", false);
        Concat c = new Concat(left, right);
        Sorts lSorts = new Sorts();
        lSorts.addSort(left);
        Sorts rSorts = new Sorts();
        rSorts.addSort(right);
        Assert.assertEquals(new Concat(propertyEqual, lSorts, linkNotNull, rSorts), getOptimizedTree(c));
        Assert.assertEquals(0, getAnalyzedSortCount(c));
        Sort s = new SortByProperty(c, "s", true);
        Assert.assertEquals(new Concat(propertyEqual, new Sorts(), linkNotNull, new Sorts()), getOptimizedTree(s));
        Assert.assertEquals(1, getAnalyzedSortCount(s));
    }

    public void testMinus() throws Exception {
        Assert.assertEquals(0, QueryUtil.getSize(instantiate(new Minus(propertyEqual, linkNotNull))));
        Minus minus = new Minus(linkNotNull, propertyEqual);
        Assert.assertEquals(2, QueryUtil.getSize(instantiate(minus)));
        Assert.assertFalse(minus.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(minus));
    }

    public void testOr() throws Exception {
        Or or = new Or(propertyEqual, linkNotNull);
        Assert.assertEquals(3, QueryUtil.getSize(instantiate(or)));
        Assert.assertFalse(or.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(or));
    }

    public void testBinaryOperation() throws Exception {
        Or tree = new Or(new UnaryNot(NodeFactory.all()), new UnaryNot(NodeFactory.all()));
        tree.replaceChild(tree.getLeft(), NodeFactory.all());
        Assert.assertEquals(new Or(NodeFactory.all(), new UnaryNot(NodeFactory.all())), tree);
        tree.replaceChild(tree.getRight(), NodeFactory.all());
        Assert.assertEquals(new Or(NodeFactory.all(), NodeFactory.all()), tree);
    }

    public void testGenericSort() throws Exception {
        ComparableGetterSort sortNode = new ComparableGetterSort(concat, new ComparableGetter() {
            @Override
            public Comparable select(Entity entity) {
                return entity.getProperty("i");
            }
        }, true);
        Assert.assertEquals(sortNode, sortNode.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortNode)));
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(new And(sortNode, NodeFactory.all()))));
        Assert.assertFalse(sortNode.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(sortNode));
    }

    public void testSortByLinkProperty() throws Exception {
        SortByLinkProperty sortByLinkProperty = new SortByLinkProperty(concat, "MyEnum", "number", "myEnum", true);
        Assert.assertEquals(sortByLinkProperty, sortByLinkProperty.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortByLinkProperty)));
        Assert.assertFalse(sortByLinkProperty.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(sortByLinkProperty));
    }

    public void testSortByProperty() throws Exception {
        SortByProperty sortByProperty = new SortByProperty(concat, "i", true);
        Assert.assertEquals(sortByProperty, sortByProperty.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortByProperty)));
        Assert.assertFalse(sortByProperty.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(sortByProperty));
    }

    public void testUnaryNot() throws Exception {
        Assert.assertEquals(new And(propertyNotNull, new Or(propertyEqual, linkNotNull)), getOptimizedTree(new And(propertyNotNull, new UnaryNot(new And(new UnaryNot(propertyEqual), new UnaryNot(linkNotNull))))));
        Assert.assertEquals(new And(propertyNotNull, new Minus(linkNotNull, propertyEqual)), getOptimizedTree(new And(propertyNotNull, new And(new UnaryNot(propertyEqual), linkNotNull))));
        Assert.assertEquals(new And(propertyNotNull, new Minus(propertyEqual, linkNotNull)), getOptimizedTree(new And(propertyNotNull, new And(propertyEqual, new UnaryNot(linkNotNull)))));
        Assert.assertEquals(new Minus(propertyNotNull, new Minus(propertyEqual, linkNotNull)), getOptimizedTree(new And(propertyNotNull, new Or(new UnaryNot(propertyEqual), linkNotNull))));
        Assert.assertEquals(new Minus(propertyNotNull, new Minus(linkNotNull, propertyEqual)), getOptimizedTree(new And(propertyNotNull, new Or(propertyEqual, new UnaryNot(linkNotNull)))));
        NodeBase node = new And(propertyNotNull, new UnaryNot(new Or(new UnaryNot(propertyEqual), new UnaryNot(linkNotNull))));
        Assert.assertEquals(new And(propertyNotNull, new And(propertyEqual, linkNotNull)), getOptimizedTree(node));
        Assert.assertFalse(node.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(node));
    }

    public void testPropertyEqual() throws Exception {
        Assert.assertEquals(propertyNotNull, getOptimizedTree(new UnaryNot(propertyEqualNull)));
        Assert.assertEquals(new Minus(propertyEqual, propertyNotNull), getOptimizedTree(new And(propertyEqualNull, propertyEqual)));
        Assert.assertEquals(new Minus(NodeFactory.all(), new Minus(propertyNotNull, propertyEqual)), getOptimizedTree(new Or(propertyEqualNull, propertyEqual)));
        Assert.assertEquals(new Minus(NodeFactory.all(), propertyNotNull), getOptimizedTree(propertyEqualNull));
        Assert.assertFalse(propertyEqual.equals(propertyEqualNull));
        Assert.assertFalse(propertyEqualNull.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(linkEqualNull));
        Assert.assertFalse(linkEqualNull.equals(propertyEqual));
    }

    public void testPropertyNotNull() throws Exception {
        Assert.assertEquals(new Minus(linkEqual, propertyNotNull), getOptimizedTree(new Minus(new Minus(linkEqual, propertyNotNull), propertyNotNull)));
        Assert.assertEquals(new Minus(linkEqual, propertyNotNull), getOptimizedTree(new Minus(new Minus(new Minus(linkEqual, propertyNotNull), propertyNotNull), propertyNotNull)));
    }

    public void testGetAll() throws Exception {
        Assert.assertEquals(propertyEqual, getOptimizedTree(new And(NodeFactory.all(), propertyEqual)));
        Assert.assertEquals(NodeFactory.all(), getOptimizedTree(new Or(NodeFactory.all(), propertyEqual)));
        Assert.assertFalse(NodeFactory.all().equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(NodeFactory.all()));
    }

    public void testLinkEqual() throws Exception {
        Assert.assertEquals(new Minus(NodeFactory.all(), new Or(linkNotNull, new LinkNotNull("self1"))), getOptimizedTree(new And(linkEqualNull, new LinkEqual("self1", null))));
        Assert.assertEquals(linkNotNull, getOptimizedTree(new UnaryNot(linkEqualNull)));
        Assert.assertEquals(new Minus(propertyEqual, linkNotNull), getOptimizedTree(new And(propertyEqual, linkEqualNull)));
        Assert.assertEquals(new Minus(NodeFactory.all(), new Minus(linkNotNull, propertyEqual)), getOptimizedTree(new Or(propertyEqual, linkEqualNull)));
        Assert.assertEquals(new Minus(NodeFactory.all(), linkNotNull), getOptimizedTree(linkEqualNull));
        Assert.assertFalse(linkEqualNull.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(linkEqualNull));
    }

    public void testLinkNotNull() throws Exception {
        Assert.assertTrue(QueryUtil.isEmpty(queryEngine, instantiate(new UnaryNot(new LinkNotNull("self1")))));
        Assert.assertFalse(linkNotNull.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(linkNotNull));
    }

    public void testPropertyStartsWith() throws Exception {
        NodeBase node = getOptimizedTree(new PropertyStartsWith("s", ""));
        Assert.assertEquals(NodeFactory.all(), node);
        node = getOptimizedTree(new PropertyStartsWith("s", null));
        Assert.assertEquals(NodeFactory.all(), node);
        Assert.assertFalse(node.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(node));
    }

    public void testPropertyRange() throws Exception {
        NodeBase node = getOptimizedTree(new And(new And(new PropertyRange("string", "aa", "pq"), new PropertyRange("string", "d", "pz")), new PropertyRange("1string", "1d", "1pz")));
        Assert.assertEquals(new And(new PropertyRange("string", "d", "pq"), new PropertyRange("1string", "1d", "1pz")), node);
        Assert.assertFalse(node.equals(propertyEqual));
        Assert.assertFalse(propertyEqual.equals(node));
    }

    public void testCloneAndAnalyze() throws Exception {
        NodeBase tree = new Root(new UnaryNot(new Concat(new Minus(getTree(queryEngine.query(null, "TstClass", new Or(new And(new PropertyEqual("i", 1), new PropertyEqual("i", 2)),
                new PropertyEqual("i", 3)))), getTree(queryEngine.query(null, "TstClass", new PropertyEqual("i", 4)))), new Concat(getTree(queryEngine.queryGetAll("TstClass")),
                new Concat(getTree(queryEngine.query(null, "TstClass", new LinkEqual("itself", QueryUtil.getFirst(queryEngine, queryEngine.queryGetAll("TstClass"))))), new Concat(new LinkNotNull("itself"),
                        new Concat(getTree(queryEngine.query(null, "TstClass", new LinksEqualDecorator("itself", new LinkEqual("self1",
                                QueryUtil.getFirst(queryEngine, queryEngine.queryGetAll("TstClass"))), "TstClass"))), new Concat(new PropertyNotNull("s"), new Concat(getTree(queryEngine.query(null, "TstClass",
                                new PropertyRange("i", QueryUtil.nextGreater(0, Integer.class), QueryUtil.positiveInfinity(Integer.class)))),
                                getTree(queryEngine.query(null, "TstClass", new PropertyStartsWith("s", "val")))
                        )))
                ))
        ))));
        NodeBase clone = tree.getClone();
        Assert.assertEquals(tree, clone);
        tree = ((UnaryNode) tree).getChild();
        Assert.assertEquals(new Minus(NodeFactory.all(), ((UnaryNode) tree).getChild().getClone()), getOptimizedTree(tree));
        ComparableGetter valueGetter = new ComparableGetter() {
            @Override
            public Comparable select(Entity entity) {
                return entity.getProperty("i");
            }
        };
        ComparableGetterSort genericSort = ComparableGetterSort.create(concat, valueGetter, true);
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(getAnalyzedSortCount(genericSort), i + 1);
            genericSort = new ComparableGetterSort(genericSort, valueGetter, true);
        }
        Assert.assertEquals(getAnalyzedSortCount(genericSort), 4);
    }

    public void testSimplify() throws Exception {
        PropertyEqual a = new PropertyEqual("s", "A");
        PropertyEqual a2 = new PropertyEqual("s", "A");
        PropertyEqual b = new PropertyEqual("s", "B");
        PropertyEqual c = new PropertyEqual("s", "C");
        PropertyEqual d = new PropertyEqual("s", "D");
        NodeBase tree = new Concat(new Concat(new And(new Or(a, b), new Or(a2, c)), new And(new Or(a, b), new Or(c, a2))), new Concat(new And(new Or(b, a), new Or(a2, c)), new And(new Or(b, a), new Or(c, a2))));
        Concat expectedOptimizedTree = new Concat(new Concat(new Or(a, new And(b, c)), new Or(a, new And(c, b))), new Concat(new Or(a, new And(b, c)), new Or(a, new And(b, c))));
        NodeBase optimizedTree = getOptimizedTree(tree);

        System.out.println("Original tree:\n" + tree);
        System.out.println("Optimized tree:\n" + optimizedTree);
        System.out.println("Expected optimized tree:\n" + expectedOptimizedTree);

        Assert.assertEquals(expectedOptimizedTree, optimizedTree);

        tree = new Concat(new Concat(new Or(new And(a, b), new And(a2, c)), new Or(new And(a, b), new And(c, a2))), new Concat(new Or(new And(b, a), new And(a2, c)), new Or(new And(b, a), new And(c, a2))));
        Assert.assertEquals(new Concat(new Concat(new And(a, new Or(b, c)), new And(a, new Or(c, b))), new Concat(new And(a, new Or(b, c)), new And(a, new Or(b, c)))), getOptimizedTree(tree));

        tree = new Minus(a, new Minus(a, b));
        Assert.assertEquals(new And(a, b), getOptimizedTree(tree));

        tree = new Or(new Or(new And(a, b), c), new And(d, b));
        Assert.assertEquals(new Or(new And(new Or(a, d), b), c), getOptimizedTree(tree));
    }

    public void testQueryOptimizationPeformance1() throws Exception {
        PropertyEqual a = new PropertyEqual("s", "A");
        PropertyEqual a2 = new PropertyEqual("s", "A");
        PropertyEqual b = new PropertyEqual("s", "B");
        PropertyEqual c = new PropertyEqual("s", "C");
        final NodeBase tree = new Concat(new Concat(new And(new Or(a, b), new Or(a2, c)), new And(new Or(a, b),
                new Or(c, a2))), new Concat(new And(new Or(b, a), new Or(a2, c)), new And(new Or(b, a), new Or(c, a2))));

        p("queryOptimizationPeformance1", new F() {
            @Override
            public void execute(int i) {
                getOptimizedTree(tree);
            }
        }, 10000);
    }

    private static NodeBase getTree(Iterable<Entity> seq) {
        return ((TreeKeepingEntityIterable) seq).getTree();
    }

    private Iterable<Entity> instantiate(NodeBase node) {
        TreeKeepingEntityIterable entityIterable = queryEngine.query("TstClass", node);
        return entityIterable.instantiate();
    }

    private NodeBase getOptimizedTree(NodeBase node) {
        TreeKeepingEntityIterable entityIterable = queryEngine.query("TstClass", node);
        entityIterable.optimize();
        return entityIterable.getOptimizedTree();
    }

    private int getAnalyzedSortCount(NodeBase node) {
        TreeKeepingEntityIterable entityIterable = queryEngine.query("TstClass", node);
        entityIterable.optimize();
        return entityIterable.getSorts().sortCount();
    }

    private void prepare() {
        final PersistentStoreTransaction txn = getStoreTransaction();
        final Entity enumeration = e(txn, 1);
        t1(txn, enumeration);
        t1(txn, enumeration);
        t2(txn, e(txn, 2), "value", 9);
        propertyEqual = new PropertyEqual("s", "value");
        propertyEqualNull = new PropertyEqual("s", null);
        propertyNotNull = new PropertyNotNull("s");
        linkEqual = new LinkEqual("myEnum", enumeration);
        linkEqualNull = new LinkEqual("itself", null);
        linkNotNull = new LinkNotNull("itself");
        concat = new Concat(propertyEqual, linkNotNull);
    }

    private Entity e(PersistentStoreTransaction txn, int number) {
        final Entity result = txn.newEntity("MyEnum");
        result.setProperty("number", number);
        return result;
    }

    private Entity t1(PersistentStoreTransaction txn, Entity enumeration) {
        final Entity e = txn.newEntity("TstClass");
        e.setLink("itself", e);
        e.setLink("myEnum", enumeration);
        e.setLink("self1", e);
        e.setLink("self2", e);
        return e;
    }

    private Entity t2(PersistentStoreTransaction txn, Entity enumeration, String s, int v) {
        final Entity e = t1(txn, enumeration);
        e.setProperty("s", s);
        e.setProperty("i", v);
        return e;

    }

    public long p(String title, final F f, int iter) {
        System.out.println("Start [" + title + ']');
        long t = System.nanoTime();
        for (int i = 0; i < iter; i++) {
            f.execute(i);
        }
        t = System.nanoTime() - t;
        double fps = 1000000000.0 / ((double) t / (double) iter);
        System.out.printf("End [%s] in %d ms, fps is %f i/sec\n", title, (t / 1000000), fps);
        return t;
    }

    private interface F {
        void execute(int i);
    }
}
