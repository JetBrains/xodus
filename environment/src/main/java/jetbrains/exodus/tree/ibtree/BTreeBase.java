package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ITree;
import org.jetbrains.annotations.NotNull;

abstract class BTreeBase implements ITree {
    public static final byte INTERNAL = 44;
    public static final byte LEAF = 45;
    public static final byte VALUE = 46;

    public static final byte INTERNAL_ROOT = 47;
    public static final byte LEAF_ROOT = 48;

    @NotNull
    protected final Log log;
    protected final int structureId;
    protected final int pageSize;


    protected BTreeBase(@NotNull Log log, int structureId, int pageSize) {
        this.log = log;
        this.structureId = structureId;
        this.pageSize = pageSize;
    }
}