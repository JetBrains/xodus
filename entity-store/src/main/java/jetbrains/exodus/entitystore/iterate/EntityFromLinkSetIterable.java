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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.LinkValue;
import jetbrains.exodus.entitystore.tables.PropertyKey;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;

public class EntityFromLinkSetIterable extends EntityLinksIterableBase {

    private final IntHashMap<String> linkNames;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                Integer linkCount = Integer.valueOf((String) parameters[2]);
                IntHashMap<String> linkNames = new IntHashMap<>(linkCount);
                for (int i = 0; i < linkCount; i++) {
                    linkNames.put(Integer.valueOf((String) parameters[4 + i]), null);
                }
                PersistentEntityId entityId = new PersistentEntityId(Integer.valueOf((String) parameters[0]),
                    Long.valueOf((String) parameters[1]));
                List<String> entityLinkNames = store.getLinkNames(txn, new PersistentEntity(store, entityId));
                for (String linkName : entityLinkNames) {
                    int linkId = store.getLinkId(txn, linkName, false);
                    if (linkNames.containsKey(linkId)) {
                        linkNames.put(linkId, linkName);
                    }
                }
                return new EntityFromLinkSetIterable(txn, entityId, linkNames);
            }
        });
    }

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    public EntityFromLinkSetIterable(@NotNull final PersistentStoreTransaction txn,
                                     @NotNull final EntityId entityId,
                                     @NotNull final IntHashMap<String> linkNames) {
        super(txn, entityId);
        this.linkNames = linkNames;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.ENTITY_FROM_LINKS_SET;
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new LinksIterator(openCursor(txn), getFirstKey());
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new ConstantEntityIterableHandle(getStore(), EntityFromLinkSetIterable.getType()) {

            @NotNull
            @Override
            public int[] getLinkIds() {
                final int linksCount = linkNames.size();
                if (linksCount == 0) {
                    return IdFilter.EMPTY_ID_ARRAY;
                }
                final int[] result = new int[linksCount];
                int i = 0;
                for (final int id : linkNames.keySet()) {
                    result[i++] = id;
                }
                Arrays.sort(result);
                return result;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                ((PersistentEntityId) entityId).toString(builder);
                builder.append('-');
                builder.append(linkNames.size());
                for (final int id : linkNames.keySet()) {
                    builder.append('-');
                    builder.append(id);
                }
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                ((PersistentEntityId) entityId).toHash(hash);
                hash.applyDelimiter();
                hash.apply(linkNames.size());
                for (final int id : linkNames.keySet()) {
                    hash.applyDelimiter();
                    hash.apply(id);
                }
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull final EntityId source,
                                              @NotNull final EntityId target,
                                              final int linkId) {
                return entityId.equals(source);
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull final EntityId source,
                                                @NotNull final EntityId target,
                                                final int linkId) {
                return entityId.equals(source);
            }
        };
    }

    @Override
    protected CachedInstanceIterable createCachedInstance(@NotNull final PersistentStoreTransaction txn) {
        final IntArrayList propIds = new IntArrayList();
        final CachedInstanceIterable cached = EntityIdArrayCachedInstanceIterableFactory.createInstance(txn, this, new LinksIterator(openCursor(txn), getFirstKey()) {
            @Override
            public EntityId nextId() {
                final EntityId result = super.nextId();
                propIds.add(currentPropId());
                return result;
            }
        });
        return new EntityIdArrayWithSetIterableWrapper(txn, cached, propIds, linkNames);
    }

    private Cursor openCursor(@NotNull final PersistentStoreTransaction txn) {
        return getStore().getLinksFirstIndexCursor(txn, entityId.getTypeId());
    }

    private ByteIterable getFirstKey() {
        return PropertyKey.propertyKeyToEntry(new PropertyKey(entityId.getLocalId(), 0));
    }

    private class LinksIterator extends EntityFromLinkSetIteratorBase {

        private boolean hasNext;
        private boolean hasNextValid;
        private int currentPropId;
        private EntityId nextId;
        private EntityIdSet idSet;

        private LinksIterator(@NotNull final Cursor index,
                              @NotNull final ByteIterable key) {
            super(EntityFromLinkSetIterable.this);
            setCursor(index);
            hasNextValid = index.getSearchKeyRange(key) == null;
            idSet = EntityIdSetFactory.newSet();
        }

        @Override
        public int currentPropId() {
            return currentPropId;
        }

        @Override
        protected String getLinkName(int linkId) {
            return linkNames.get(linkId);
        }

        @Override
        protected boolean hasNextImpl() {
            if (!hasNextValid) {
                hasNext = hasNextProp();
                hasNextValid = true;
            }
            return hasNext;
        }

        @Override
        protected EntityId nextIdImpl() {
            if (hasNextImpl()) {
                explain(getType());
                if (getCursor().getNext()) {
                    hasNextValid = false;
                } else {
                    hasNext = false;
                }
                return nextId;
            }
            return null;
        }

        private boolean hasNextProp() {
            final Cursor index = getCursor();
            do {
                final PropertyKey prop = PropertyKey.entryToPropertyKey(index.getKey());
                if (prop.getEntityLocalId() != entityId.getLocalId()) {
                    return false;
                }
                final int propId = prop.getPropertyId();
                if (linkNames.containsKey(propId)) {
                    final LinkValue value = LinkValue.entryToLinkValue(getCursor().getValue());
                    final EntityId result = value.getEntityId();
                    if (!idSet.contains(result)) {
                        nextId = result;
                        idSet = idSet.add(result);
                        currentPropId = propId;
                        return true;
                    }
                }
            } while (index.getNext());
            return false;
        }
    }
}
