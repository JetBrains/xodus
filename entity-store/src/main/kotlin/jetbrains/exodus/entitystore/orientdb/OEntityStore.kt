package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.*
import java.util.concurrent.Executor

interface OEntityStore : EntityStore {

    /**
     * Executor service used to compute count of entities in entity iterable asynchronously.
     */
    val countExecutor: Executor
}
