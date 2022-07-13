package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Presentation of immutable leaf page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableLeafPage extends ImmutableBasePage {
    ImmutableLeafPage(@NotNull Log log, @NotNull ByteBuffer page,  long pageAddress) {
        super(log, page, pageAddress);
    }

    ByteBuffer getValue(final int index) {
        final long valueAddress = getChildAddress(index);
        var loggable = log.read(valueAddress);

        assert loggable.getType() == BTreeBase.VALUE_NODE;

        var data = loggable.getData();
        return data.getByteBuffer();
    }
}
