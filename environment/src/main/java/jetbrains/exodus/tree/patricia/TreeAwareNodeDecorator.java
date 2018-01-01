/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.patricia;


import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

final class TreeAwareNodeDecorator extends NodeBase {

    @NotNull
    private final PatriciaTreeBase tree;
    @NotNull
    private final NodeBase decorated;

    TreeAwareNodeDecorator(@NotNull PatriciaTreeBase tree, @NotNull NodeBase decorated) {
        super(decorated.keySequence, decorated.value);
        this.tree = tree;
        this.decorated = decorated;
    }

    @NotNull
    PatriciaTreeBase getTree() {
        return tree;
    }

    @Override
    long getAddress() {
        return decorated.getAddress();
    }

    @Override
    boolean isMutable() {
        return decorated.isMutable();
    }

    @Override
    MutableNode getMutableCopy(@NotNull final PatriciaTreeMutable mutableTree) {
        return decorated.getMutableCopy(mutableTree);
    }

    @Override
    NodeBase getChild(@NotNull final PatriciaTreeBase tree, final byte b) {
        return decorated.getChild(tree, b);
    }

    @NotNull
    @Override
    NodeChildrenIterator getChildren(final byte b) {
        return decorated.getChildren(b);
    }

    @NotNull
    @Override
    NodeChildrenIterator getChildrenRange(final byte b) {
        return decorated.getChildrenRange(b);
    }

    @NotNull
    @Override
    NodeChildrenIterator getChildrenLast() {
        return decorated.getChildrenLast();
    }

    @NotNull
    @Override
    NodeChildren getChildren() {
        return decorated.getChildren();
    }

    @Override
    int getChildrenCount() {
        return decorated.getChildrenCount();
    }

    @Override
    public void dump(PrintStream out, int level, @Nullable ToString renderer) {
        dump(tree, this, out, level, renderer);
    }

    private static void dump(PatriciaTreeBase tree, NodeBase node, PrintStream out, int level, @Nullable ToString renderer) {
        out.println(String.format("node %s%s] %s",
                node.isMutable() ? "(m) [" : '[',
                node.getChildrenCount(),
                renderer == null ? node.toString() : renderer.toString(node)
        ));
        for (ChildReference child : node.getChildren()) {
            indent(out, level);
            final long childAddress = child.suffixAddress;
            out.println(child.firstByte + " -> " + childAddress);
            indent(out, level + 1);
            out.print("+ ");
            if (childAddress == Loggable.NULL_ADDRESS || tree.getLog().hasAddress(childAddress)) {
                dump(tree, child.getNode(tree), out, level + 3, renderer);
            } else {
                out.println("[not found]");
            }
        }
    }
}
