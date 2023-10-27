/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.btree;

import org.jetbrains.annotations.NotNull;

public class BTreeMutatingTraverser extends BTreeTraverser {

    @NotNull
    protected final BTreeMutable mainTree;

    protected BTreeMutatingTraverser(@NotNull BTreeMutable mainTree) {
        super(mainTree.getRoot());
        this.mainTree = mainTree;
    }

    @Override
    protected ILeafNode pushChild(@NotNull TreePos topPos, @NotNull BasePage child, int pos) {
        return super.pushChild(topPos, child.getMutableCopy(mainTree), pos);
    }

    @NotNull
    public static BTreeMutatingTraverser create(@NotNull BTreeBase mainTree) {
        return new BTreeMutatingTraverser(mainTree.getMutableCopy());
    }
}
