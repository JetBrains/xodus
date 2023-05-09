/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.tree.btree

open class BTreeBalancePolicy @JvmOverloads constructor(val pageMaxSize: Int, val dupPageMaxSize: Int = pageMaxSize) {

    /**
     * @param page page to check whether it has to be split.
     * @return true if specified page has to be split before inserting new item.
     */
    fun needSplit(page: BasePage): Boolean {
        return page.size >= if (isDupTree(page)) dupPageMaxSize else pageMaxSize
    }

    /**
     * @param page           page to be split.
     * @param insertPosition position to insert a new item.
     * @return split position.
     */
    open fun getSplitPos(page: BasePage, insertPosition: Int): Int {
        // if inserting into the most right position - split as 8/1, otherwise - 1/1
        val pageSize = page.size
        return if (insertPosition < pageSize) pageSize shr 1 else pageSize * 7 shr 3
    }

    /**
     * Is invoked on the leaf deletion only.
     *
     * @param left  left page.
     * @param right right page.
     * @return true if the left page ought to be merged with the right one.
     */
    fun needMerge(left: BasePage, right: BasePage): Boolean {
        val leftSize = left.size
        val rightSize = right.size
        return leftSize == 0 || rightSize == 0 || leftSize + rightSize <= (if (isDupTree(left)) dupPageMaxSize else pageMaxSize) * 7 shr 3
    }

    companion object {
        var DEFAULT = BTreeBalancePolicy(128, 32)
        private fun isDupTree(page: BasePage): Boolean {
            return (page.tree as BTreeMutable).isDup()
        }
    }
}
