package jetbrains.exodus.query;

import jetbrains.exodus.entitystore.youtrackdb.gremlin.GremlinQuery;

import javax.annotation.Nullable;

public interface GremlinNode {
    // Can return null if
    @Nullable
    GremlinQuery getQuery();
}
