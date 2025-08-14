package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinBlock;

import javax.annotation.Nullable;

public interface GremlinNode {
    // Can return null if
    @Nullable
    GremlinBlock getBlock();
}
