package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;

final class SingleKeyCursorIsEmptyChecker {

    private final boolean isEmpty;

    SingleKeyCursorIsEmptyChecker(@NotNull final Cursor cursor, @NotNull final ByteIterable keyEntry) {
        try {
            isEmpty = cursor.getSearchKey(keyEntry) == null;
        } finally {
            cursor.close();
        }
    }

    public boolean isEmpty() {
        return isEmpty;
    }
}
