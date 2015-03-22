package jetbrains.exodus.entitystore;

import jetbrains.exodus.entitystore.iterate.ConstantEntityIterableHandle;
import jetbrains.exodus.entitystore.iterate.EntityIterableHandleBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

public class EntityIterableHandleTests extends EntityStoreTestBase {

    public void testTrivial() {
        final EntityIterableHandleBase h = new ConstantEntityIterableHandle(getEntityStore(), EntityIterableType.EMPTY) {
            @Override
            protected void hashCode(@NotNull EntityIterableHandleHash hash) {
                for (int i = 0; i < 31; ++i) {
                    hash.apply("0");
                }
            }
        };
        Assert.assertEquals("00000000000000000000000000000000", h.toString());
    }
}
