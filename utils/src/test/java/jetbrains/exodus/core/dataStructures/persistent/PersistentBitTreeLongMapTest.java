package jetbrains.exodus.core.dataStructures.persistent;

import org.jetbrains.annotations.NotNull;

public class PersistentBitTreeLongMapTest extends PersistentLongMapTest {

    @NotNull
    @Override
    protected PersistentLongMap<String> createMap() {
        return new PersistentBitTreeLongMap<>();
    }
}
