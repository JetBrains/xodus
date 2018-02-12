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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.SingleEntityIterable;
import jetbrains.exodus.query.metadata.AssociationEndMetaData;
import jetbrains.exodus.query.metadata.EntityMetaData;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;

@SuppressWarnings("UnusedParameters")
public class QueryEngine {

    private final ModelMetaData mmd;
    private final PersistentEntityStoreImpl persistentStore;
    private final UniqueKeyIndicesEngine ukiEngine;
    private SortEngine sortEngine;

    public QueryEngine(final ModelMetaData mmd, final PersistentEntityStoreImpl persistentStore) {
        this.mmd = mmd;
        this.persistentStore = persistentStore;
        ukiEngine = new MetaDataAwareUniqueKeyIndicesEngine(persistentStore, mmd);
    }

    protected Iterable<Entity> inMemorySelectDistinct(Iterable<Entity> it, final String linkName) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    protected Iterable<Entity> inMemorySelectManyDistinct(Iterable<Entity> it, final String linkName) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    protected Iterable<Entity> inMemoryIntersect(Iterable<Entity> left, Iterable<Entity> right) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    protected Iterable<Entity> inMemoryUnion(Iterable<Entity> left, Iterable<Entity> right) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    protected Iterable<Entity> inMemoryConcat(Iterable<Entity> left, Iterable<Entity> right) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    protected Iterable<Entity> inMemoryExclude(Iterable<Entity> left, Iterable<Entity> right) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    public ModelMetaData getModelMetaData() {
        return mmd;
    }

    public PersistentEntityStoreImpl getPersistentStore() {
        return persistentStore;
    }

    public UniqueKeyIndicesEngine getUniqueKeyIndicesEngine() {
        return ukiEngine;
    }

    public SortEngine getSortEngine() {
        return sortEngine;
    }

    public void setSortEngine(SortEngine sortEngine) {
        this.sortEngine = sortEngine;
    }

    protected void assertOperational() {
    }

    protected boolean isWrapped(@Nullable Iterable<Entity> it) {
        return true;
    }

    @NotNull
    protected EntityIterable wrap(@NotNull EntityIterable it) {
        return it;
    }

    @Nullable
    protected Iterable<Entity> wrap(@NotNull Entity entity) {
        return new SingleEntityIterable(persistentStore.getAndCheckCurrentTransaction(), entity.getId());
    }

    public boolean isPersistentIterable(Iterable<Entity> it) {
        return it instanceof EntityIterableBase;
    }

    public TreeKeepingEntityIterable queryGetAll(String entityType) {
        return query(null, entityType, NodeFactory.all());
    }

    public TreeKeepingEntityIterable query(@NotNull String entityType, @NotNull final NodeBase tree) {
        return query(null, entityType, tree);
    }

    public TreeKeepingEntityIterable query(@Nullable final Iterable<Entity> instance, @NotNull String entityType, @NotNull final NodeBase tree) {
        return new TreeKeepingEntityIterable(instance, entityType, tree, this);
    }

    public Iterable<Entity> intersect(Iterable<Entity> left, Iterable<Entity> right) {
        if (left == right) {
            return left;
        }
        if (isEmptyIterable(left) || isEmptyIterable(right)) {
            return EntityIterableBase.EMPTY;
        }
        if (left instanceof TreeKeepingEntityIterable) {
            final TreeKeepingEntityIterable l = (TreeKeepingEntityIterable) left;
            final String leftType = l.getEntityType();
            if (right instanceof TreeKeepingEntityIterable) {
                final TreeKeepingEntityIterable r = (TreeKeepingEntityIterable) right;
                if (l.getInstance() == r.getInstance()) {
                    final String rightType = r.getEntityType();
                    if (Utils.isTypeOf(leftType, rightType, mmd)) {
                        return new TreeKeepingEntityIterable(r.getInstance(), leftType, new And(l.getTree(), r.getTree()), l.annotatedTree, r.annotatedTree, this);
                    } else if (Utils.isTypeOf(rightType, leftType, mmd)) {
                        return new TreeKeepingEntityIterable(r.getInstance(), rightType, new And(l.getTree(), r.getTree()), l.annotatedTree, r.annotatedTree, this);
                    }
                }
            }
        }
        String staticType = null;
        if (left instanceof StaticTypedEntityIterable) {
            final StaticTypedEntityIterable l = (StaticTypedEntityIterable) left;
            final String leftType = l.getEntityType();
            if (right instanceof StaticTypedEntityIterable) {
                final StaticTypedEntityIterable r = (StaticTypedEntityIterable) right;
                final String rightType = r.getEntityType();
                if (Utils.isTypeOf(rightType, leftType, mmd)) {
                    staticType = rightType;
                } else if (Utils.isTypeOf(leftType, rightType, mmd)) {
                    staticType = leftType;
                }
                if (leftType.equals(rightType)) {
                    if (isGetAllTree(l)) {
                        return new ExcludeNullStaticTypedEntityIterable(rightType, r, this);
                    }
                    if (isGetAllTree(r)) {
                        return new ExcludeNullStaticTypedEntityIterable(leftType, l, this);
                    }
                }
            }
        }
        right = instantiateAndAdjust(right);
        left = instantiateAndAdjust(left);
        final Iterable<Entity> result = intersectNonTrees(left, right);
        return staticType != null ? new StaticTypedIterableDecorator(staticType, result, this) : result;
    }

    public Iterable<Entity> union(Iterable<Entity> left, Iterable<Entity> right) {
        if (left == right) {
            return left;
        }
        if (isEmptyIterable(left)) {
            return right;
        }
        if (isEmptyIterable(right)) {
            return left;
        }
        if (left instanceof TreeKeepingEntityIterable) {
            final TreeKeepingEntityIterable l = (TreeKeepingEntityIterable) left;
            String leftType = l.getEntityType();
            if (right instanceof TreeKeepingEntityIterable) {
                final TreeKeepingEntityIterable r = (TreeKeepingEntityIterable) right;
                if (l.getInstance() == r.getInstance() && leftType.equals(r.getEntityType())) {
                    return new TreeKeepingEntityIterable(r.getInstance(), leftType, new Or(l.getTree(), r.getTree()), l.annotatedTree, r.annotatedTree, this);
                }
            }
        }
        String staticType = null;
        if (left instanceof StaticTypedEntityIterable) {
            final StaticTypedEntityIterable l = (StaticTypedEntityIterable) left;
            final String leftType = l.getEntityType();
            if (right instanceof StaticTypedEntityIterable) {
                final StaticTypedEntityIterable r = (StaticTypedEntityIterable) right;
                final String rightType = r.getEntityType();
                if (leftType.equals(rightType)) {
                    staticType = rightType;
                    if (isGetAllTree(l)) {
                        return new AddNullStaticTypedEntityIterable(staticType, l, r, this);
                    }
                    if (isGetAllTree(r)) {
                        return new AddNullStaticTypedEntityIterable(staticType, r, l, this);
                    }
                }
            }
        }
        right = instantiateAndAdjust(right);
        left = instantiateAndAdjust(left);
        final Iterable<Entity> result = unionNonTrees(left, right);
        return staticType != null ? new StaticTypedIterableDecorator(staticType, result, this) : result;
    }

    public Iterable<Entity> concat(Iterable<Entity> left, Iterable<Entity> right) {
        if (isEmptyIterable(left)) {
            return right;
        }
        if (isEmptyIterable(right)) {
            return left;
        }
        if (left instanceof TreeKeepingEntityIterable) {
            final TreeKeepingEntityIterable l = (TreeKeepingEntityIterable) left;
            String leftType = l.getEntityType();
            if (right instanceof TreeKeepingEntityIterable) {
                final TreeKeepingEntityIterable r = (TreeKeepingEntityIterable) right;
                if (l.getInstance() == r.getInstance() && leftType.equals(r.getEntityType())) {
                    return new TreeKeepingEntityIterable(r.getInstance(), leftType, new Concat(l.getTree(), r.getTree()),
                        l.annotatedTree, r.annotatedTree, this);
                }
            }
        }
        final String staticType = retrieveStaticType(left, right);
        right = instantiateAndAdjust(right);
        left = instantiateAndAdjust(left);
        final Iterable<Entity> result = concatNonTrees(left, right);
        return staticType != null ? new StaticTypedIterableDecorator(staticType, result, this) : result;
    }

    public Iterable<Entity> exclude(Iterable<Entity> left, Iterable<Entity> right) {
        if (isEmptyIterable(left) || left == right) {
            return EntityIterableBase.EMPTY;
        }
        if (isEmptyIterable(right)) {
            return left;
        }
        if (left instanceof TreeKeepingEntityIterable) {
            final TreeKeepingEntityIterable l = (TreeKeepingEntityIterable) left;
            String leftType = l.getEntityType();
            if (right instanceof TreeKeepingEntityIterable) {
                final TreeKeepingEntityIterable r = (TreeKeepingEntityIterable) right;
                if (l.getInstance() == r.getInstance() && leftType.equals(r.getEntityType())) {
                    return new TreeKeepingEntityIterable(r.getInstance(), leftType, new Minus(l.getTree(), r.getTree()), l.annotatedTree, r.annotatedTree, this);
                }
            }
        }
        final String staticType = retrieveStaticType(left, right);
        right = instantiateAndAdjust(right);
        left = instantiateAndAdjust(left);
        final Iterable<Entity> result = excludeNonTrees(left, right);
        return staticType != null ? new StaticTypedIterableDecorator(staticType, result, this) : result;
    }

    public Iterable<Entity> selectDistinct(Iterable<Entity> it, final String linkName) {
        if (it == null) {
            return EntityIterableBase.EMPTY;
        }
        if (mmd != null) {
            if (it instanceof StaticTypedEntityIterable) {
                final StaticTypedEntityIterable ktei = (StaticTypedEntityIterable) it;
                it = toEntityIterable(it);
                if (isPersistentIterable(it)) {
                    final String entityType = ktei.getEntityType();
                    final EntityMetaData emd = mmd.getEntityMetaData(entityType);
                    if (emd != null) {
                        final AssociationEndMetaData aemd = emd.getAssociationEndMetaData(linkName);
                        if (aemd != null) {
                            final String resultType = aemd.getOppositeEntityMetaData().getType();
                            return new StaticTypedIterableDecorator(resultType,
                                selectDistinctImpl((EntityIterableBase) it, linkName), this);
                        }
                    }
                }
            } else if (isPersistentIterable(it)) {
                return selectDistinctImpl((EntityIterableBase) it, linkName);
            }
        }
        return inMemorySelectDistinct(it, linkName);
    }

    public Iterable<Entity> selectManyDistinct(Iterable<Entity> it, final String linkName) {
        if (it == null) {
            return EntityIterableBase.EMPTY;
        }
        if (mmd != null) {
            if (it instanceof StaticTypedEntityIterable) {
                final StaticTypedEntityIterable tree = (StaticTypedEntityIterable) it;
                it = toEntityIterable(it);
                if (isPersistentIterable(it)) {
                    final String entityType = tree.getEntityType();
                    final EntityMetaData emd = mmd.getEntityMetaData(entityType);
                    if (emd != null) {
                        final AssociationEndMetaData aemd = emd.getAssociationEndMetaData(linkName);
                        if (aemd != null) {
                            final String resultType = aemd.getOppositeEntityMetaData().getType();
                            return new StaticTypedIterableDecorator(resultType,
                                selectManyDistinctImpl((EntityIterableBase) it, linkName), this);
                        }
                    }
                }
            } else if (isPersistentIterable(it)) {
                return selectManyDistinctImpl((EntityIterableBase) it, linkName);
            }
        }
        return inMemorySelectManyDistinct(it, linkName);
    }

    public Iterable<Entity> toEntityIterable(Iterable<Entity> it) {
        if (it instanceof StaticTypedEntityIterable) {
            it = ((StaticTypedEntityIterable) it).instantiate();
        }
        return adjustEntityIterable(it);
    }

    public Iterable<Entity> intersectNonTrees(Iterable<Entity> left, Iterable<Entity> right) {
        if (isPersistentIterable(left) && isPersistentIterable(right)) {
            return wrap(((EntityIterableBase) left).getSource().intersect(((EntityIterableBase) right).getSource()));
        }
        return inMemoryIntersect(left, right);
    }

    public Iterable<Entity> unionNonTrees(Iterable<Entity> left, Iterable<Entity> right) {
        if (isPersistentIterable(left) && isPersistentIterable(right)) {
            return wrap(((EntityIterableBase) left).getSource().union(((EntityIterableBase) right).getSource()));
        }
        return inMemoryUnion(left, right);
    }

    public Iterable<Entity> concatNonTrees(Iterable<Entity> left, Iterable<Entity> right) {
        if (isPersistentIterable(left) && isPersistentIterable(right)) {
            return wrap(((EntityIterableBase) left).getSource().concat(((EntityIterableBase) right).getSource()));
        }
        return inMemoryConcat(left, right);
    }

    public Iterable<Entity> excludeNonTrees(Iterable<Entity> left, Iterable<Entity> right) {
        if (isPersistentIterable(left) && isPersistentIterable(right)) {
            return wrap(((EntityIterableBase) left).getSource().minus(((EntityIterableBase) right).getSource()));
        }
        // subtract
        return inMemoryExclude(left, right);
    }

    private Iterable<Entity> instantiateAndAdjust(Iterable<Entity> it) {
        return adjustEntityIterable(StaticTypedEntityIterable.instantiate(it));
    }

    private Iterable<Entity> selectDistinctImpl(final EntityIterableBase it, String linkName) {
        assertOperational();
        return wrap(it.getSource().selectDistinct(linkName));
    }

    private Iterable<Entity> selectManyDistinctImpl(final EntityIterableBase it, String linkName) {
        assertOperational();
        return wrap(it.getSource().selectManyDistinct(linkName));
    }

    public Iterable<Entity> adjustEntityIterable(Iterable<Entity> it) {
        if (it == EntityIterableBase.EMPTY) {
            return it;
        }
        // try to convert collection to entity iterable.
        if (it instanceof Collection) {
            final Collection<Entity> collection = (Collection<Entity>) it;
            final Iterator<Entity> itr = collection.iterator();
            if (itr.hasNext()) {
                final Entity e = itr.next();
                if (!itr.hasNext()) {
                    final Iterable<Entity> wrapped = wrap(e);
                    if (wrapped != null) {
                        it = wrapped;
                    }
                }
            } else {
                return EntityIterableBase.EMPTY;
            }
        }
        // wrap with transient iterable
        return isPersistentIterable(it) && !isWrapped(it) ? wrap(((EntityIterableBase) it).getSource()) : it;
    }

    public Iterable<Entity> unionAdjusted(final Iterable<Entity> left, final Iterable<Entity> right) {
        return unionNonTrees(adjustEntityIterable(left), adjustEntityIterable(right));
    }

    public Iterable<Entity> intersectAdjusted(final Iterable<Entity> left, final Iterable<Entity> right) {
        return intersectNonTrees(adjustEntityIterable(left), adjustEntityIterable(right));
    }

    public Iterable<Entity> concatAdjusted(final Iterable<Entity> left, final Iterable<Entity> right) {
        return concatNonTrees(adjustEntityIterable(left), adjustEntityIterable(right));
    }

    public Iterable<Entity> excludeAdjusted(final Iterable<Entity> left, final Iterable<Entity> right) {
        return excludeNonTrees(adjustEntityIterable(left), adjustEntityIterable(right));
    }

    public EntityIterable instantiateGetAll(@NotNull final String entityType) {
        return getPersistentStore().getAndCheckCurrentTransaction().getAll(entityType);
    }

    public static boolean isEmptyIterable(Iterable<Entity> it) {
        return it == null || it == EntityIterableBase.EMPTY || (it instanceof StaticTypedIterableDecorator && ((StaticTypedIterableDecorator) it).getDecorated() == EntityIterableBase.EMPTY);
    }

    public static boolean isGetAllTree(final StaticTypedEntityIterable tree) {
        return tree instanceof TreeKeepingEntityIterable && ((TreeKeepingEntityIterable) tree).getTree() instanceof GetAll;
    }

    private static String retrieveStaticType(Iterable<Entity> left, Iterable<Entity> right) {
        if (left instanceof StaticTypedEntityIterable) {
            final String leftType = ((StaticTypedEntityIterable) left).getEntityType();
            if (right instanceof StaticTypedEntityIterable) {
                final String rightType = ((StaticTypedEntityIterable) right).getEntityType();
                if (leftType.equals(rightType)) {
                    return rightType;
                }
            }
        }
        return null;
    }
}
