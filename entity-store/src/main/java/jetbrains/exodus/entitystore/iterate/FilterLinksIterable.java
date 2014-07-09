package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.util.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FilterLinksIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new FilterLinksIterable(
                        store, (String) parameters[0], (EntityIterableBase) parameters[1], (EntityIterable) parameters[2]);
            }
        });
    }

    @NotNull
    private final String linkName;
    @NotNull
    private final EntityIterable entities;

    public FilterLinksIterable(@NotNull final PersistentEntityStoreImpl store,
                               @NotNull final String linkName,
                               @NotNull final EntityIterableBase source,
                               @NotNull final EntityIterable entities) {
        super(store, source);
        this.linkName = linkName;
        this.entities = entities;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.FILTER_LINKS;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new NonDisposableEntityIterator(this) {

            @NotNull
            private final EntityIteratorBase sourceIt = (EntityIteratorBase) source.iterator();
            @Nullable
            private EntityId nextId = PersistentEntityId.EMPTY_ID;
            @Nullable
            private EntityIdSet idSet = null;

            @Override
            protected boolean hasNextImpl() {
                if (nextId != PersistentEntityId.EMPTY_ID) {
                    return true;
                }
                while (sourceIt.hasNext()) {
                    nextId = sourceIt.nextId();
                    if (nextId != null) {
                        final Entity link = getEntity(nextId).getLink(linkName);
                        if (link != null && getIdSet().contains(link.getId())) {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            protected EntityId nextIdImpl() {
                final EntityId result = nextId;
                nextId = PersistentEntityId.EMPTY_ID;
                return result;
            }

            @NotNull
            private EntityIdSet getIdSet() {
                if (idSet == null) {
                    idSet = ((EntityIterableBase) entities).toSet(txn);
                }
                return idSet;
            }
        });
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), getType(), source.getHandle()) {
            @Override
            public void getStringHandle(@NotNull final StringBuilder builder) {
                super.getStringHandle(builder);
                builder.append('-');
                builder.append(linkName);
                builder.append('-');
                decorated.getStringHandle(builder);
                builder.append('-');
                entities.getHandle().getStringHandle(builder);
            }
        };
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    public boolean canBeCached() {
        return false;
    }
}
