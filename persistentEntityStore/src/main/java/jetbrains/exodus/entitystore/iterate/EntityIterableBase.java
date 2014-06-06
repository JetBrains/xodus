/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.binop.ConcatenationIterable;
import jetbrains.exodus.entitystore.iterate.binop.IntersectionIterable;
import jetbrains.exodus.entitystore.iterate.binop.MinusIterable;
import jetbrains.exodus.entitystore.iterate.binop.UnionIterable;
import jetbrains.exodus.entitystore.util.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

@SuppressWarnings({"InstanceofThis", "DynamicRegexReplaceableByCompiledPattern", "HardcodedLineSeparator"})
public abstract class EntityIterableBase implements EntityIterable {

    public static final EntityIterableBase EMPTY;
    public static final Map<EntityIterableType, EntityIterableInstantiator> INSTANTIATORS = new HashMap<EntityIterableType, EntityIterableInstantiator>();

    static {
        EMPTY = new EntityIterableBase(null) {
            @Override
            @NotNull
            public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
                return EntityIteratorBase.EMPTY;
            }

            @Override
            public boolean setOrigin(Object origin) {
                return true;
            }

            @Override
            @NotNull
            protected EntityIterableHandle getHandleImpl() {
                //noinspection EmptyClass
                return new ConstantEntityIterableHandle(null, EntityIterableType.EMPTY) {
                };
            }

            @Override
            public int indexOf(@NotNull Entity entity) {
                return -1;
            }

            @Override
            protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
                return 0;
            }

            @Override
            public boolean canBeCached() {
                return false;
            }
        };

        registerType(EntityIterableType.EMPTY, new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return EMPTY;
            }
        });
    }

    private static final String INDENT = "|   ";
    // amount of fields appended to iterable string handle
    static final int[] fields = {0, 1, 2, 3, 3, 4, 2, 2, 2, 3, 4, 2, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 1, 1, 2, 2, 2, 0, 0, 1, 3};
    // amount of childern recursively appended to iterable string handle 
    static final int[] children = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 1, 2, 2, 0, 1, 1, 1, 1, 1, 0, 0, 0, 2, 1, 1, 0};

    @Nullable
    private final PersistentEntityStoreImpl store;
    private EntityIterableHandle cachedHandle;
    private Object origin;
    @NotNull
    protected TxnGetterStategy txnGetter = TxnGetterStategy.DEFAULT;

    protected EntityIterableBase(@Nullable final PersistentEntityStoreImpl store) {
        this.store = store;
    }

    @SuppressWarnings({"NullableProblems"})
    @NotNull
    public PersistentEntityStoreImpl getStore() {
        if (store == null) {
            throw new RuntimeException("EntityIterableBase: entity store is not set.");
        }
        return store;
    }

    @Override
    public EntityIterator iterator() {
        // EMPTY iterable
        if (store == null) {
            return EntityIteratorBase.EMPTY;
        }
        final PersistentStoreTransaction txn = getTransaction();
        final EntityIterator result = store.getEntityIterableCache().putIfNotCached(this).getIteratorImpl(txn);
        if (result.shouldBeDisposed()) {
            txn.registerEntityIterator(result);
        }
        return result;
    }

    @NotNull
    public abstract EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn);

    public EntityIterator getIteratorImpl() {
        return getIteratorImpl(getTransaction());
    }

    @NotNull
    @Override
    public PersistentStoreTransaction getTransaction() {
        return txnGetter.getTxn(this);
    }

    public boolean hasCustomTxn() {
        return txnGetter != TxnGetterStategy.DEFAULT;
    }

    @Override
    public boolean isEmpty() {
        // EMPTY iterable
        if (store == null) {
            return true;
        }
        final EntityIteratorBase it = (EntityIteratorBase) store.getEntityIterableCache().putIfNotCached(this).getIteratorImpl();
        try {
            return !it.hasNext();
        } finally {
            it.disposeIfShouldBe();
        }
    }

    public boolean nonCachedHasFastCount() {
        return false;
    }

    @Override
    public long size() {
        if (store == null) {
            return 0;
        }
        final PersistentStoreTransaction txn = getTransaction();
        final EntityIterableBase it = store.getEntityIterableCache().putIfNotCached(this);
        final EntityIterableBase cached = it.nonCachedHasFastCount() ? it : getOrCreateCachedWrapper(txn);
        return cached.countImpl(txn);
    }

    @Override
    public long count() {
        if (store == null) {
            return 0;
        }
        final EntityIterableBase it = store.getEntityIterableCache().putIfNotCached(this);
        return it.isCachedWrapper() ? it.countImpl(getTransaction()) : -1;
    }

    @Override
    public long getRoughCount() {
        if (store == null) {
            return 0;
        }
        final EntityIterableCacheImpl cache = store.getEntityIterableCache();
        final EntityIterableBase it = cache.putIfNotCached(this);
        return it.isCachedWrapper() ? it.countImpl(getTransaction()) : cache.getCachedCount(getHandle());
    }

    @Override
    public int indexOf(@NotNull final Entity entity) {
        if (store == null) {
            return -1;
        }
        final EntityId entityId = entity.getId();
        final EntityIterableBase it = store.getEntityIterableCache().putIfNotCached(this);
        final EntityIterableBase cached = it.isCachedWrapper() ? it : this.getOrCreateCachedWrapper(getTransaction());
        return cached.indexOfImpl(entityId);
    }

    @Override
    public boolean contains(@NotNull final Entity entity) {
        return indexOf(entity) >= 0;
    }

    @Override
    @NotNull
    public final EntityIterableHandle getHandle() {
        if (cachedHandle == null) {
            cachedHandle = getHandleImpl();
        }
        return cachedHandle;
    }

    @NotNull
    protected abstract EntityIterableHandle getHandleImpl();

    public Object getOrigin() {
        return origin;
    }

    public boolean setOrigin(Object origin) {
        if (getStore().getExplainer().isExplainOn() && this.origin == null) {
            this.origin = origin;
            return true;
        }
        return false;
    }

    @Override
    @NotNull
    public EntityIterable intersect(@NotNull final EntityIterable right) {
        if (this == EMPTY || right == EMPTY) {
            return EMPTY;
        }
        return new IntersectionIterable(store, this, (EntityIterableBase) right);
    }

    @Override
    @NotNull
    public EntityIterable intersectSavingOrder(@NotNull final EntityIterable right) {
        if (this == EMPTY || right == EMPTY) {
            return EMPTY;
        }
        return new IntersectionIterable(store, this, (EntityIterableBase) right, true);
    }

    @Override
    @NotNull
    public EntityIterable union(@NotNull final EntityIterable right) {
        if (this == EMPTY) {
            return right;
        }
        if (right == EMPTY) {
            return this;
        }
        return new UnionIterable(store, this, (EntityIterableBase) right);
    }

    @Override
    @NotNull
    public EntityIterable minus(@NotNull final EntityIterable right) {
        if (this == EMPTY) {
            return EMPTY;
        }
        if (right == EMPTY) {
            return this;
        }
        return new MinusIterable(store, this, (EntityIterableBase) right);
    }

    @Override
    @NotNull
    public EntityIterable concat(@NotNull final EntityIterable right) {
        if (this == EMPTY) {
            return right;
        }
        if (right == EMPTY) {
            return this;
        }
        // try to build right-oriented trees
        if (this instanceof ConcatenationIterable) {
            final ConcatenationIterable thisConcat = (ConcatenationIterable) this;
            return new ConcatenationIterable(store, thisConcat.getLeft(),
                    new ConcatenationIterable(store, thisConcat.getRight(), (EntityIterableBase) right));
        }
        return new ConcatenationIterable(store, this, (EntityIterableBase) right);
    }

    @NotNull
    @Override
    public final EntityIterableBase skip(final int number) {
        if (number <= 0 || store == null) {
            return this;
        }
        return new SkipEntityIterable(store, this, number);
    }

    @NotNull
    @Override
    public EntityIterable take(int number) {
        if (number <= 0 || store == null) {
            return EMPTY;
        }
        return new TakeEntityIterable(store, this, number);
    }

    @NotNull
    @Override
    public EntityIterable distinct() {
        if (store == null) {
            return EMPTY;
        }
        return new DistinctIterable(store, this);
    }

    @NotNull
    @Override
    public EntityIterable selectDistinct(@NotNull final String linkName) {
        if (isDecoratorForSelectDistinct(this)) {
            final EntityIterableDecoratorBase decorator = (EntityIterableDecoratorBase) this;
            return decorator.getDecorated().selectDistinct(linkName);
        }
        if (store == null) {
            return EMPTY;
        }
        final int linkId = store.getLinkId(getTransaction(), linkName, false);
        return new SelectDistinctIterable(store, this, linkId);
    }

    @NotNull
    @Override
    public EntityIterable selectManyDistinct(@NotNull final String linkName) {
        if (isDecoratorForSelectDistinct(this)) {
            final EntityIterableDecoratorBase decorator = (EntityIterableDecoratorBase) this;
            return decorator.getDecorated().selectManyDistinct(linkName);
        }
        if (store == null) {
            return EMPTY;
        }
        final int linkId = store.getLinkId(getTransaction(), linkName, false);
        return new SelectManyDistinctIterable(store, this, linkId);
    }

    @Nullable
    @Override
    public Entity getFirst() {
        final EntityIterator it = iterator();
        if (it.hasNext()) {
            try {
                final EntityId id = it.nextId();
                if (id != null) {
                    return getEntity(id);
                }
            } finally {
                if (it instanceof EntityIteratorBase) {
                    ((EntityIteratorBase) it).disposeIfShouldBe();
                }
            }
        }
        return null;
    }

    @Nullable
    @Override
    public Entity getLast() {
        final EntityIteratorBase it = (EntityIteratorBase) iterator();
        final EntityId id = it.getLast();
        it.disposeIfShouldBe();
        return id == null ? null : getEntity(id);
    }

    @NotNull
    @Override
    public EntityIterable reverse() {
        if (store == null) {
            return EMPTY;
        }
        return new EntityReverseIterable(store, this);
    }

    @Override
    public boolean isSortResult() {
        // EMPTY can be a result of sorting
        return this == EMPTY || this instanceof SortResultIterable;
    }

    @NotNull
    @Override
    public EntityIterable asSortResult() {
        return store == null ? this : new SortResultIterable(store, this);
    }

    @Override
    @NotNull
    public EntityIterable getSource() {
        return this;
    }

    public boolean isSortedById() {
        return true;
    }

    public int depth() {
        return 1;
    }

    /**
     * Should this type of iterables be cached.
     *
     * @return true if should.
     */
    public boolean canBeCached() {
        return true;
    }

    @NotNull
    public Entity getEntity(@NotNull final EntityId id) {
        return getStore().getEntity(id);
    }

    public boolean isCachedWrapper() {
        return false;
    }

    public final CachedWrapperIterable getOrCreateCachedWrapper(@NotNull final PersistentStoreTransaction txn) {
        final boolean canBeCached = store != null && store.isCachingEnabled() && canBeCached();
        if (!canBeCached) {
            return createCachedWrapper(txn);
        }
        CachedWrapperIterable cached = txn.getCachedWrapper(this);
        if (cached == null) {
            cached = createCachedWrapper(txn);
            txn.addCachedWrapper(cached);
        }
        return cached;
    }

    public EntityIdSet toSet(@NotNull final PersistentStoreTransaction txn) {
        return getOrCreateCachedWrapper(txn).toSet(txn);
    }

    @NotNull
    public EntityIterator getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        throw new UnsupportedOperationException("getReverseIterator not implemented");
    }

    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        final EntityIterator it = getIteratorImpl(txn);
        long result = 0;
        while (it.hasNext()) {
            ++result;
            it.nextId();
        }
        return result;
    }

    protected int indexOfImpl(@NotNull final EntityId entityId) {
        int result = 0;
        final EntityIteratorBase it = (EntityIteratorBase) getIteratorImpl();
        while (it.hasNext()) {
            final EntityId nextId = it.nextId();
            if (nextId != null && nextId.equals(entityId)) {
                it.disposeIfShouldBe();
                return result;
            }
            ++result;
        }
        return -1;
    }

    protected CachedWrapperIterable createCachedWrapper(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIdArrayIterableWrapper(txn, getStore(), this);
    }

    public static String getHumanReadablePresentation(String handle) {
        String[] types = handle.split("-");
        int minus = 0;
        for (int i = 0; i < types.length; i++) {
            if (types[i].isEmpty()) {
                minus++;
                types[i + 1] = '-' + types[i + 1];
            } else {
                types[i - minus] = types[i];
            }
        }
        types = Arrays.copyOf(types, types.length - minus);
        int[] pos = {1};
        StringBuilder presentation = new StringBuilder();
        getHumanReadablePresentation(presentation, types, pos, "");
        if (pos[0] < types.length - 1) {
            throw new RuntimeException("Whole handle not read.\n" + presentation);
        }
        return presentation.toString();
    }

    private static void getHumanReadablePresentation(StringBuilder presentation, String[] types, int[] pos, String indent) {
        int type = Integer.valueOf(types[pos[0]]);
        pos[0]++;
        if (type < 0 || type >= children.length) {
            throw new RuntimeException("New EntityIterable added: " + type);
        }
        presentation.append(indent).append(EntityIterableType.values()[type].getDescription());
        for (int i = 0; i < fields[type]; i++) {
            presentation.append(' ').append(types[pos[0]]);
            if (type == EntityIterableType.SINGLE_ENTITY.getType() && "null".equals(types[pos[0]])) {
                break;
            }
            pos[0]++;
        }
        StringBuilder tmp = new StringBuilder();
        for (int i = 0; i < children[type]; i++) {
            tmp.append('\n');
            getHumanReadablePresentation(tmp, types, pos, indent + INDENT);
        }
        if (type == EntityIterableType.SELECT_DISTINCT.getType() ||
                type == EntityIterableType.SELECTMANY_DISTINCT.getType() ||
                type == EntityIterableType.SORTING.getType()) {
            presentation.append(' ').append(types[pos[0]]);
            pos[0]++;
        }
        presentation.append(tmp);
        if (type == EntityIterableType.MERGE_SORTED.getType()) {
            int count = Integer.valueOf(types[pos[0]]);
            presentation.append(' ').append(count);
            pos[0]++;
            for (int i = 0; i < count; i++) {
                pos[0]++;
                presentation.append('\n');
                getHumanReadablePresentation(presentation, types, pos, indent + INDENT);
            }
        }
        if (type == EntityIterableType.ENTITY_FROM_LINKS_SET.getType() ||
                type == EntityIterableType.HISTORY_ENTITY_FROM_LINKS_SET.getType()) {
            int count = Integer.valueOf(types[pos[0]]);
            presentation.append("  ").append(count).append(" links:");
            pos[0]++;
            for (int i = 0; i < count; i++) {
                presentation.append(' ').append(types[pos[0]]);
                pos[0]++;
            }
        }
    }

    protected static void registerType(EntityIterableType type, EntityIterableInstantiator instantiator) {
        INSTANTIATORS.put(type, instantiator);
    }

    private static boolean isDecoratorForSelectDistinct(@NotNull final EntityIterable source) {
        return source instanceof SortIterable || source instanceof SortIndirectIterable ||
                source instanceof EntityReverseIterable || source instanceof DistinctIterable ||
                source instanceof SortResultIterable;
    }

    public static EntityIterableBase instantiate(final PersistentStoreTransaction txn, PersistentEntityStoreImpl store, String presentation) {
        return instantiate(txn, store, presentation.split("\n"), 0);
    }

    private static EntityIterableBase instantiate(final PersistentStoreTransaction txn, PersistentEntityStoreImpl store, String[] presentation, int line) {
        Integer[] childrenLines = getChildren(presentation, line);
        String s = presentation[line].substring(getIndent(presentation[line]));
        EntityIterableType type = getTypeByDescription(s);
        s = s.substring(type.getDescription().length());
        String[] parameters = getParameters(s);
        Object[] constructorParameters = new Object[parameters.length + childrenLines.length];
        //noinspection ManualArrayCopy
        for (int i = 0; i < parameters.length; i++) {
            constructorParameters[i] = parameters[i];
        }
        for (int i = 0; i < childrenLines.length; i++) {
            constructorParameters[i + parameters.length] = instantiate(txn, store, presentation, childrenLines[i]);
        }
        return INSTANTIATORS.get(type).instantiate(txn, store, constructorParameters);
    }

    private static String[] getParameters(String s) {
        String[] result = s.trim().split(" ");
        int empty = 0;
        for (int i = 0; i < result.length; i++) {
            if (result[i].isEmpty()) {
                empty++;
            } else {
                result[i - empty] = result[i];
            }
        }
        return Arrays.copyOf(result, result.length - empty);
    }

    private static EntityIterableType getTypeByDescription(String s) {
        EntityIterableType result = null;
        int length = 0;
        for (EntityIterableType type : EntityIterableType.values()) {
            String description = type.getDescription();
            if (s.startsWith(description) && description.length() > length) {
                length = description.length();
                result = type;
            }
        }
        return result;
    }

    private static Integer[] getChildren(String[] presentation, int line) {
        int indent = getIndent(presentation[line]);
        int childIndent = indent + INDENT.length();
        ArrayList<Integer> result = new ArrayList<Integer>();
        while (++line < presentation.length) {
            int lineIndent = getIndent(presentation[line]);
            if (lineIndent == childIndent) {
                result.add(line);
            } else if (lineIndent < childIndent) {
                break;
            }
        }
        return result.toArray(new Integer[result.size()]);
    }

    private static int getIndent(String s) {
        int indent = 0;
        while (s.substring(indent).startsWith(INDENT)) {
            indent += INDENT.length();
        }
        return indent;
    }

    public void explain(EntityIterableType type) {
        if (getOrigin() != null) {
            Explainer explainer = getStore().getExplainer();
            explainer.explain(getOrigin(), Explainer.CURSOR_ADVANCES);
            explainer.explain(getOrigin(), Explainer._CURSOR_ADVANCES_BY_TYPE + ' ' + type.name());
            explainer.explain(getOrigin(), Explainer._CURSOR_ADVANCES_BY_HANDLE + ' ' + getHandle().getStringHandle());
        }
    }
}
