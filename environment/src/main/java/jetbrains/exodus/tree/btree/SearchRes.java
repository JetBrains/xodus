/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import org.jetbrains.annotations.Nullable;

/**
 * Result of binary search
 */
public class SearchRes {

    public static final SearchRes NOT_FOUND = new SearchRes(-1);

    public final int index;
    public final ILeafNode key;

    public SearchRes(int index, @Nullable ILeafNode key) {
        this.index = index;
        this.key = key;
    }

    public SearchRes(int index) {
        this(index, null);
    }

}
