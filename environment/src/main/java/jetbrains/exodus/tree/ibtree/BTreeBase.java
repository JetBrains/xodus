package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

abstract class BTreeBase {
    static final int LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET = 2 * Long.BYTES;

    public static final byte INTERNAL_PAGE = 44;
    public static final byte LEAF_PAGE = 45;
    public static final byte VALUE_NODE = 46;
    public static final byte KEY_NODE = 47;

    @NotNull
    final Log log;
    final int structureId;
    final int pageSize;

    protected BTreeBase(@NotNull Log log, int structureId, int pageSize) {
        this.log = log;
        this.structureId = structureId;
        this.pageSize = pageSize;
    }
}