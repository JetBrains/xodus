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
package jetbrains.exodus.query;


import org.jetbrains.annotations.NotNull;

public abstract class CommutativeOperator extends BinaryOperator {
    CommutativeOperator(@NotNull final NodeBase left, @NotNull final NodeBase right) {
        super(left, right);
    }

    public void flipChildren() {
        NodeBase _left = getLeft();
        setLeft(getRight());
        setRight(_left);
    }
}
