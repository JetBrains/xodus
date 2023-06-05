package jetbrains.exodus.newLogConcept.tree;


import jetbrains.exodus.ByteIterable;
import java.util.concurrent.ConcurrentSkipListSet;

public class Tree {

    public final ConcurrentSkipListSet<TreeEntry> tree = new ConcurrentSkipListSet<>();

    public ByteIterable searchInTree(ByteIterable key) {
        TreeEntry searchEntry = new TreeEntry(key, null);
        TreeEntry foundEntry = tree.floor(searchEntry);

        if (foundEntry != null && foundEntry.getKey().equals(key)) {
            return foundEntry.getValue();
        }
        return null;
    }

}

