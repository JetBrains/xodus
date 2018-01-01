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

import org.jetbrains.annotations.NotNull;

public class BTreeBalancePolicy {

    public static BTreeBalancePolicy DEFAULT = new BTreeBalancePolicy(256);

    private final int maxSize;

    public BTreeBalancePolicy(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getPageMaxSize() {
        return maxSize;
    }

    /**
     * @param page page to check whether it has to be split.
     * @return true if specified page has to be split before inserting new item.
     */
    public boolean needSplit(@NotNull final BasePage page) {
        return page.getSize() >= getPageMaxSize();
    }

    /**
     * @param page           page to be split.
     * @param insertPosition position to insert a new item.
     * @return split position.
     */
    public int getSplitPos(@NotNull final BasePage page, final int insertPosition) {
        // if inserting into the most right position - split as 8/1, otherwise - 1/1
        final int pageSize = page.getSize();
        return insertPosition < pageSize ? pageSize >> 1 : (pageSize * 7) >> 3;
    }

    /**
     * Is invoked on the leaf deletion only.
     *
     * @param left  left page.
     * @param right right page.
     * @return true if the left page ought to be merged with the right one.
     */
    public boolean needMerge(@NotNull final BasePage left, @NotNull final BasePage right) {
        final int leftSize = left.getSize();
        final int rightSize = right.getSize();
        return leftSize == 0 || rightSize == 0 || leftSize + rightSize <= ((getPageMaxSize() * 7) >> 3);
    }
}
