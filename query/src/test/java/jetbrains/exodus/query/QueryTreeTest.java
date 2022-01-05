/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.query;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.entitystore.ComparableGetter;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityStoreTestBase;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.junit.Assert;

import java.util.Objects;

import static jetbrains.exodus.query.And.and;
import static jetbrains.exodus.query.Or.or;
import static jetbrains.exodus.query.metadata.AssociationEndCardinality._0_1;
import static jetbrains.exodus.query.metadata.MetaBuilder.*;

@SuppressWarnings({"HardcodedLineSeparator", "OverlyCoupledMethod"})
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

    public void testAnd() {
        NodeBase and = and(propertyEqual, linkNotNull);
        Assert.assertEquals(1, QueryUtil.getSize(instantiate(and)));
        Assert.assertNotEquals(and, propertyEqual);
        Assert.assertNotEquals(propertyEqual, and);
    }

    public void testConcat() {
        Assert.assertEquals(3, QueryUtil.getSize(queryEngine.queryGetAll("TstClass")));
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(concat)));
        Assert.assertNotEquals(concat, propertyEqual);
        Assert.assertNotEquals(propertyEqual, concat);

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

    public void testMinus() {
        Assert.assertEquals(0, QueryUtil.getSize(instantiate(new Minus(propertyEqual, linkNotNull))));
        Minus minus = new Minus(linkNotNull, propertyEqual);
        Assert.assertEquals(2, QueryUtil.getSize(instantiate(minus)));
        Assert.assertNotEquals(minus, propertyEqual);
        Assert.assertNotEquals(propertyEqual, minus);
    }

    public void testOr() {
        NodeBase or = or(propertyEqual, linkNotNull);
        Assert.assertEquals(3, QueryUtil.getSize(instantiate(or)));
        Assert.assertNotEquals(or, propertyEqual);
        Assert.assertNotEquals(propertyEqual, or);
    }

    public void testBinaryOperation() {
        Or tree = (Or) or(new UnaryNot(propertyEqual), new UnaryNot(linkNotNull));
        tree.replaceChild(tree.getLeft(), propertyEqual);
        Assert.assertEquals(or(propertyEqual, new UnaryNot(linkNotNull)), tree);
        tree.replaceChild(tree.getRight(), linkNotNull);
        Assert.assertEquals(or(propertyEqual, linkNotNull), tree);
    }

    public void testGenericSort() {
        ComparableGetterSort sortNode = new ComparableGetterSort(concat, entity -> entity.getProperty("i"), true);
        Assert.assertEquals(sortNode, sortNode.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortNode)));
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(and(sortNode, NodeFactory.all()))));
        Assert.assertNotEquals(sortNode, propertyEqual);
        Assert.assertNotEquals(propertyEqual, sortNode);
    }

    public void testSortByLinkProperty() {
        SortByLinkProperty sortByLinkProperty = new SortByLinkProperty(concat, "MyEnum", "number", "myEnum", true);
        Assert.assertEquals(sortByLinkProperty, sortByLinkProperty.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortByLinkProperty)));
        Assert.assertNotEquals(sortByLinkProperty, propertyEqual);
        Assert.assertNotEquals(propertyEqual, sortByLinkProperty);
    }

    public void testSortByProperty() {
        SortByProperty sortByProperty = new SortByProperty(concat, "i", true);
        Assert.assertEquals(sortByProperty, sortByProperty.getClone());
        Assert.assertEquals(4, QueryUtil.getSize(instantiate(sortByProperty)));
        Assert.assertNotEquals(sortByProperty, propertyEqual);
        Assert.assertNotEquals(propertyEqual, sortByProperty);
    }

    public void testUnaryNot() {
        Assert.assertEquals(and(propertyNotNull, or(propertyEqual, linkNotNull)), getOptimizedTree(and(propertyNotNull, new UnaryNot(and(new UnaryNot(propertyEqual), new UnaryNot(linkNotNull))))));
        Assert.assertEquals(and(propertyNotNull, new Minus(linkNotNull, propertyEqual)), getOptimizedTree(and(propertyNotNull, and(new UnaryNot(propertyEqual), linkNotNull))));
        Assert.assertEquals(and(propertyNotNull, new Minus(propertyEqual, linkNotNull)), getOptimizedTree(and(propertyNotNull, and(propertyEqual, new UnaryNot(linkNotNull)))));
        Assert.assertEquals(new Minus(propertyNotNull, new Minus(propertyEqual, linkNotNull)), getOptimizedTree(and(propertyNotNull, or(new UnaryNot(propertyEqual), linkNotNull))));
        Assert.assertEquals(new Minus(propertyNotNull, new Minus(linkNotNull, propertyEqual)), getOptimizedTree(and(propertyNotNull, or(propertyEqual, new UnaryNot(linkNotNull)))));
        NodeBase node = and(propertyNotNull, new UnaryNot(or(new UnaryNot(propertyEqual), new UnaryNot(linkNotNull))));
        Assert.assertEquals(and(propertyNotNull, and(propertyEqual, linkNotNull)), getOptimizedTree(node));
        Assert.assertNotEquals(node, propertyEqual);
        Assert.assertNotEquals(propertyEqual, node);
    }

    public void testPropertyEqual() {
        Assert.assertEquals(propertyNotNull, getOptimizedTree(new UnaryNot(propertyEqualNull)));
        Assert.assertEquals(new Minus(propertyEqual, propertyNotNull), getOptimizedTree(and(propertyEqualNull, propertyEqual)));
        Assert.assertEquals(new Minus(NodeFactory.all(), new Minus(propertyNotNull, propertyEqual)), getOptimizedTree(or(propertyEqualNull, propertyEqual)));
        Assert.assertEquals(new Minus(NodeFactory.all(), propertyNotNull), getOptimizedTree(propertyEqualNull));
        Assert.assertNotEquals(propertyEqual, propertyEqualNull);
        Assert.assertNotEquals(propertyEqualNull, propertyEqual);
        Assert.assertNotEquals(propertyEqual, linkEqualNull);
        Assert.assertNotEquals(linkEqualNull, propertyEqual);
    }

    public void testPropertyNotNull() {
        Assert.assertEquals(new Minus(linkEqual, propertyNotNull), getOptimizedTree(new Minus(new Minus(linkEqual, propertyNotNull), propertyNotNull)));
        Assert.assertEquals(new Minus(linkEqual, propertyNotNull), getOptimizedTree(new Minus(new Minus(new Minus(linkEqual, propertyNotNull), propertyNotNull), propertyNotNull)));
    }

    public void testGetAll() {
        Assert.assertEquals(propertyEqual, getOptimizedTree(and(NodeFactory.all(), propertyEqual)));
        Assert.assertEquals(NodeFactory.all(), getOptimizedTree(or(NodeFactory.all(), propertyEqual)));
        Assert.assertNotEquals(NodeFactory.all(), propertyEqual);
        Assert.assertNotEquals(propertyEqual, NodeFactory.all());
    }

    public void testLinkEqual() {
        Assert.assertEquals(new Minus(NodeFactory.all(), or(linkNotNull, new LinkNotNull("self1"))), getOptimizedTree(and(linkEqualNull, new LinkEqual("self1", null))));
        Assert.assertEquals(linkNotNull, getOptimizedTree(new UnaryNot(linkEqualNull)));
        Assert.assertEquals(new Minus(propertyEqual, linkNotNull), getOptimizedTree(and(propertyEqual, linkEqualNull)));
        Assert.assertEquals(new Minus(NodeFactory.all(), new Minus(linkNotNull, propertyEqual)), getOptimizedTree(or(propertyEqual, linkEqualNull)));
        Assert.assertEquals(new Minus(NodeFactory.all(), linkNotNull), getOptimizedTree(linkEqualNull));
        Assert.assertNotEquals(linkEqualNull, propertyEqual);
        Assert.assertNotEquals(propertyEqual, linkEqualNull);
    }

    public void testLinkNotNull() {
        Assert.assertTrue(QueryUtil.isEmpty(queryEngine, instantiate(new UnaryNot(new LinkNotNull("self1")))));
        Assert.assertNotEquals(linkNotNull, propertyEqual);
        Assert.assertNotEquals(propertyEqual, linkNotNull);
    }

    public void testPropertyStartsWith() {
        NodeBase node = getOptimizedTree(new PropertyStartsWith("s", ""));
        Assert.assertEquals(NodeFactory.all(), node);
        node = getOptimizedTree(new PropertyStartsWith("s", null));
        Assert.assertEquals(NodeFactory.all(), node);
        Assert.assertNotEquals(node, propertyEqual);
        Assert.assertNotEquals(propertyEqual, node);
    }

    public void testPropertyContains() {
        NodeBase node = getOptimizedTree(new PropertyContains("s", "", true));
        Assert.assertEquals(NodeFactory.all(), node);
        node = getOptimizedTree(new PropertyContains("s", null, true));
        Assert.assertEquals(NodeFactory.all(), node);
        Assert.assertNotEquals(node, propertyEqual);
        Assert.assertNotEquals(propertyEqual, node);
    }

    public void testPropertyRange() {
        NodeBase node = getOptimizedTree(and(and(new PropertyRange("string", "aa", "pq"), new PropertyRange("string", "d", "pz")), new PropertyRange("1string", "1d", "1pz")));
        Assert.assertEquals(and(new PropertyRange("string", "d", "pq"), new PropertyRange("1string", "1d", "1pz")), node);
        Assert.assertNotEquals(node, propertyEqual);
        Assert.assertNotEquals(propertyEqual, node);
    }

    public void testCloneAndAnalyze() {
        NodeBase tree = new Root(new UnaryNot(new Concat(new Minus(getTree(queryEngine.query(null, "TstClass", or(and(new PropertyEqual("i", 1), new PropertyEqual("i", 2)),
            new PropertyEqual("i", 3)))), getTree(queryEngine.query(null, "TstClass", new PropertyEqual("i", 4)))), new Concat(getTree(queryEngine.queryGetAll("TstClass")),
            new Concat(getTree(queryEngine.query(null, "TstClass", new LinkEqual("itself", QueryUtil.getFirst(queryEngine, queryEngine.queryGetAll("TstClass"))))), new Concat(new LinkNotNull("itself"),
                new Concat(getTree(queryEngine.query(null, "TstClass", new LinksEqualDecorator("itself", new LinkEqual("self1",
                    QueryUtil.getFirst(queryEngine, queryEngine.queryGetAll("TstClass"))), "TstClass"))), new Concat(new PropertyNotNull("s"), new Concat(getTree(queryEngine.query(null, "TstClass",
                    new PropertyRange("i", Objects.requireNonNull(QueryUtil.nextGreater(0, Integer.class)), Objects.requireNonNull(QueryUtil.positiveInfinity(Integer.class))))),
                    getTree(queryEngine.query(null, "TstClass", new PropertyStartsWith("s", "val")))
                )))
            ))
        ))));
        NodeBase clone = tree.getClone();
        Assert.assertEquals(tree, clone);
        tree = ((UnaryNode) tree).getChild();
        Assert.assertEquals(new Minus(NodeFactory.all(), ((UnaryNode) tree).getChild().getClone()), getOptimizedTree(tree));
        ComparableGetter valueGetter = entity -> entity.getProperty("i");
        ComparableGetterSort genericSort = ComparableGetterSort.create(concat, valueGetter, true);
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(getAnalyzedSortCount(genericSort), i + 1);
            genericSort = new ComparableGetterSort(genericSort, valueGetter, true);
        }
        Assert.assertEquals(getAnalyzedSortCount(genericSort), 4);
    }

    public void testSimplify() {
        PropertyEqual a = new PropertyEqual("s", "A");
        PropertyEqual a2 = new PropertyEqual("s", "A");
        PropertyEqual b = new PropertyEqual("s", "B");
        PropertyEqual c = new PropertyEqual("s", "C");
        PropertyEqual d = new PropertyEqual("s", "D");
        NodeBase tree = new Concat(new Concat(and(or(a, b), or(a2, c)), and(or(a, b), or(c, a2))), new Concat(and(or(b, a), or(a2, c)), and(or(b, a), or(c, a2))));
        Concat expectedOptimizedTree = new Concat(new Concat(or(a, and(b, c)), or(a, and(c, b))), new Concat(or(a, and(b, c)), or(a, and(b, c))));
        NodeBase optimizedTree = getOptimizedTree(tree);

        System.out.println("Original tree:\n" + tree);
        System.out.println("Optimized tree:\n" + optimizedTree);
        System.out.println("Expected optimized tree:\n" + expectedOptimizedTree);

        Assert.assertEquals(expectedOptimizedTree, optimizedTree);

        tree = new Concat(new Concat(or(and(a, b), and(a2, c)), or(and(a, b), and(c, a2))), new Concat(or(and(b, a), and(a2, c)), or(and(b, a), and(c, a2))));
        Assert.assertEquals(new Concat(new Concat(and(a, or(b, c)), and(a, or(c, b))), new Concat(and(a, or(b, c)), and(a, or(b, c)))), getOptimizedTree(tree));

        tree = new Minus(a, new Minus(a, b));
        Assert.assertEquals(and(a, b), getOptimizedTree(tree));

        tree = or(or(and(a, b), c), and(d, b));
        Assert.assertEquals(or(and(or(a, d), b), c), getOptimizedTree(tree));
    }

    @TestFor(issue = "XD-694")
    public void testNestedAnd() {
        final PropertyEqual a = new PropertyEqual("s", "A");
        final PropertyEqual b = new PropertyEqual("s", "B");
        final PropertyEqual c = new PropertyEqual("s", "C");
        final NodeBase tree = and(and(and(a, or(b, c)), or(b, c)), or(b, c));
        final NodeBase optimizedTree = getOptimizedTree(tree);
        final NodeBase expectedOptimizedTree = and(a, or(b, c));
        System.out.println("Original tree:\n" + tree);
        System.out.println("Optimized tree:\n" + optimizedTree);
        System.out.println("Expected optimized tree:\n" + expectedOptimizedTree);
        Assert.assertEquals(expectedOptimizedTree, optimizedTree);
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
        Assert.assertNotNull(txn);
        final Entity enumeration = e(txn, 1);
        t1(txn, enumeration);
        t1(txn, enumeration);
        t2(txn, e(txn, 2));
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

    private static Entity t1(PersistentStoreTransaction txn, Entity enumeration) {
        final Entity e = txn.newEntity("TstClass");
        e.setLink("itself", e);
        e.setLink("myEnum", enumeration);
        e.setLink("self1", e);
        e.setLink("self2", e);
        return e;
    }

    private static void t2(PersistentStoreTransaction txn, Entity enumeration) {
        final Entity e = t1(txn, enumeration);
        e.setProperty("s", "value");
        e.setProperty("i", 9);
    }
}
