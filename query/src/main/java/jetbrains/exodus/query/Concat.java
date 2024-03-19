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


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.query.Utils.safe_equals;

public class Concat extends BinaryOperator {
    private Sorts leftSorts;
    private Sorts rightSorts;

    public Concat(@NotNull final NodeBase left, @NotNull final NodeBase right) {
        super(left, right);
    }

    public Concat(final NodeBase left, Sorts lSorts, final NodeBase right, Sorts rSorts) {
        super(left, right);
        leftSorts = lSorts;
        rightSorts = rSorts;
    }

    @Override
    public Iterable<Entity> instantiate(String entityType, QueryEngine queryEngine, ModelMetaData metaData, InstantiateContext context) {
        return queryEngine.concatAdjusted(leftSorts.apply(entityType, getLeft().instantiate(entityType, queryEngine, metaData, context), queryEngine),
                rightSorts.apply(entityType, getRight().instantiate(entityType, queryEngine, metaData, context), queryEngine));
    }

    @Override
    public void optimize(Sorts sorts, OptimizationPlan rules) {
        if (leftSorts == null) {
            leftSorts = new Sorts();
        }
        if (rightSorts == null) {
            rightSorts = new Sorts();
        }
        boolean applied = true;
        while (applied) {
            applied = false;
            final NodeBase left = getLeft();
            final NodeBase right = getRight();
            left.optimize(leftSorts, rules);
            right.optimize(rightSorts, rules);
            for (OptimizationRule rule : rules.rules) {
                applied = left.replaceIfMatches(rule) || right.replaceIfMatches(rule);
                if (applied) {
                    break;
                }
            }
        }
    }

    @Override
    public void cleanSorts(Sorts sorts) {
        if (sorts.sortCount() > 0) {
            leftSorts = new Sorts();
            rightSorts = new Sorts();
            getLeft().cleanSorts(sorts);
            getRight().cleanSorts(sorts);
        } else {
            getLeft().cleanSorts(leftSorts);
            getRight().cleanSorts(rightSorts);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        checkWildcard(obj);
        if (!(obj instanceof Concat)) {
            return false;
        }
        if (!(super.equals(obj))) {
            return false;
        }
        Concat s = (Concat) obj;
        boolean leftEmpty = (leftSorts == null || leftSorts.sortCount() == 0) && (s.leftSorts == null || s.leftSorts.sortCount() == 0);
        boolean rightEmpty = (rightSorts == null || rightSorts.sortCount() == 0) && (s.rightSorts == null || s.rightSorts.sortCount() == 0);
        return (leftEmpty || safe_equals(leftSorts, s.leftSorts)) && (rightEmpty || safe_equals(rightSorts, s.rightSorts));
    }

    @Override
    public NodeBase getClone() {
        return new Concat(getLeft().getClone(), leftSorts, getRight().getClone(), rightSorts);
    }

    @Override
    public String getSimpleName() {
        return "cnct";
    }
}
