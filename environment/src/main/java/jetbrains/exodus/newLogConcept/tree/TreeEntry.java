package jetbrains.exodus.newLogConcept.tree;

import jetbrains.exodus.ByteIterable;
import org.jetbrains.annotations.NotNull;

public class TreeEntry implements Comparable<TreeEntry> {
    private ByteIterable key;
    private ByteIterable value;

    public TreeEntry(ByteIterable key, ByteIterable value) {
        this.key = key;
        this.value = value;
    }
    public ByteIterable getKey() {
        return key;
    }

    public void setKey(ByteIterable key) {
        this.key = key;
    }

    public ByteIterable getValue() {
        return value;
    }

    public void setValue(ByteIterable value) {
        this.value = value;
    }
    @Override
    public int compareTo(@NotNull TreeEntry another) {
        return this.getKey().compareTo(another.getKey());
    }
}
