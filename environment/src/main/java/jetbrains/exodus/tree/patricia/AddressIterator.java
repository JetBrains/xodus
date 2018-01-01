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

import jetbrains.exodus.tree.LongIterator;
import jetbrains.exodus.tree.TreeTraverser;
import org.jetbrains.annotations.NotNull;

public class AddressIterator implements LongIterator {

    @NotNull
    private final TreeTraverser traverser;

    private boolean finished;

    /*
     * Used for trees with returning rootAddress
     */
    public AddressIterator(@NotNull TreeTraverser traverser) {
        this.traverser = traverser;
        finished = false;
        this.traverser.init(true);
    }

    @Override
    public boolean hasNext() {
        return !finished;
    }

    @Override
    public long next() {
        long result = traverser.getCurrentAddress();
        if (traverser.canMoveDown()) { // System.out.println("d");
            traverser.moveDown();
            return result;
        }
        if (traverser.canMoveRight()) {
            traverser.moveRight();
            traverser.moveDown(); // System.out.println("0rd");
            return result;
        }
        while (true) {
            if (traverser.canMoveUp()) {
                if (traverser.canMoveRight()) {
                    traverser.moveRight();
                    traverser.moveDown(); // System.out.println("1rd");
                    return result;
                } else { // System.out.println("u");
                    traverser.moveUp();
                }
            } else if (traverser.canMoveRight()) {
                traverser.moveRight();
                traverser.moveDown(); // System.out.println("2rd");
                return result;
            } else {
                finished = true;
                break;
            }
        }
        return result;
    }

    @NotNull
    public TreeTraverser getTraverser() {
        return traverser;
    }

    public void skipSubTree() {
        //TODO: implement (for Reflect utility only)
    }

}
