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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.LongIterator;
import jetbrains.exodus.tree.TreeTraverser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/*
 *Iterator over tree addresses
 */
public class AddressIterator implements LongIterator {

    @NotNull
    private final TreeTraverser traverser;

    @Nullable
    private ITree root;
    private boolean canGoDown = true;
    private boolean alreadyIn;

    public AddressIterator(@Nullable ITree root, boolean alreadyIn, @NotNull TreeTraverser traverser) {
        this.root = root;
        this.alreadyIn = alreadyIn;
        this.traverser = traverser;
    }

    @Override
    public boolean hasNext() {
        return traverser.canMoveRight() || advance() || root != null;
    }

    @Override
    public long next() {
        if (alreadyIn) {
            alreadyIn = false;
            return traverser.getCurrentAddress();
        }
        if (canGoDown) {
            if (traverser.canMoveDown()) {
                traverser.moveDown();
                return traverser.getCurrentAddress();
            }
        } else {
            canGoDown = true;
        }
        if (traverser.canMoveRight()) {
            traverser.moveRight();
            final long result = traverser.getCurrentAddress();
            if (traverser.canMoveDown()) {
                traverser.moveDown();
                alreadyIn = true;
            }
            return result;
        }
        if (traverser.canMoveUp()) {
            traverser.moveUp();
            canGoDown = false;
            return traverser.getCurrentAddress();
        }
        if (root != null) {
            final long result = root.getRootAddress();
            root = null;
            return result;
        }
        return Loggable.NULL_ADDRESS;
    }

    @NotNull
    public TreeTraverser getTraverser() {
        return traverser;
    }

    public void skipSubTree() {
        // TODO: implement (for Reflect utility only)
    }

    protected boolean advance() {
        while (traverser.canMoveUp()) {
            if (traverser.canMoveRight()) {
                return true;
            } else {
                traverser.moveUp();
                canGoDown = false;
            }
        }

        return traverser.canMoveRight();
    }
}
