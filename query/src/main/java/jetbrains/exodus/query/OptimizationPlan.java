/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
package jetbrains.exodus.query;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OptimizationPlan {
    private static final OptimizationPlan prepare = new OptimizationPlan(false);
    private static final OptimizationPlan optimize = new OptimizationPlan(false);
    private static final OptimizationPlan pushNotUpAndGenMinus = new OptimizationPlan(false);
    private static final OptimizationPlan removeSingleNot = new OptimizationPlan(false);
    public static final Iterable<OptimizationPlan> PLANS;

    /* package */ final List<OptimizationRule> rules;
    /**
     * true means that rules from this plan will be applied on enter during depth-first search of the QueryTree
     */
    public final boolean applyOnEnter;

    static {
        final Collection<OptimizationPlan> plans = new ArrayList<OptimizationPlan>(4);
        plans.add(prepare);
        plans.add(optimize);
        plans.add(pushNotUpAndGenMinus);
        plans.add(removeSingleNot);
        PLANS = plans;

        // a \ b == a & !b
        prepare.add(new Minus(new Wildcard(0), new Wildcard(1)), new And(new Wildcard(0), new UnaryNot(new Wildcard(1))));

        // ( a | b ) & ( a | c ) == a | ( b & c )
        optimize.add(new And(new Or(new Wildcard(0), new Wildcard(1)), new Or(new Wildcard(0), new Wildcard(2))), new Or(new Wildcard(0), new And(new Wildcard(1), new Wildcard(2))));
        // ( a & b ) | ( a & c ) == a & ( b | c )
        optimize.add(new Or(new And(new Wildcard(0), new Wildcard(1)), new And(new Wildcard(0), new Wildcard(2))), new And(new Wildcard(0), new Or(new Wildcard(1), new Wildcard(2))));
        // all & a == a
        optimize.add(new And(NodeFactory.all(), new Wildcard(0)), new Wildcard(0));
        // all | a == all
        optimize.add(new Or(NodeFactory.all(), new Wildcard(0)), NodeFactory.all());
        // a & !( a & !b ) == a & b
        // needed for optimizing a \ ( a \ b ) which is a & b
        optimize.add(new And(new Wildcard(0), new UnaryNot(new And(new Wildcard(0), new UnaryNot(new Wildcard(1))))), new And(new Wildcard(0), new Wildcard(1)));
        // for optimizing #Unassigned #Unresolved when no project is selected (if some assignee bundles differ)
        // ( ( a & b ) | c ) | ( d & b ) == ( ( a | d ) & b ) | c
        optimize.add(new Or(new Or(new And(new Wildcard(0), new Wildcard(1)), new Wildcard(2)), new And(new Wildcard(3), new Wildcard(1))), new Or(new And(new Or(new Wildcard(0), new Wildcard(3)), new Wildcard(1)), new Wildcard(2)));
        // ( a <= x <= b && c <= x <= d ) == ( max( a, c ) <= x <= min( b, d ) )
        optimize.add(new MergePropertyRanges(0), new MergePropertyRanges(0));

        // !!a == a
        pushNotUpAndGenMinus.add(new UnaryNot(new UnaryNot(new Wildcard(0))), new Wildcard(0));
        // !a & !b == !( a | b )
        pushNotUpAndGenMinus.add(new And(new UnaryNot(new Wildcard(0)), new UnaryNot(new Wildcard(1))), new UnaryNot(new Or(new Wildcard(0), new Wildcard(1))));
        // !a | !b == !( a & b )
        pushNotUpAndGenMinus.add(new Or(new UnaryNot(new Wildcard(0)), new UnaryNot(new Wildcard(1))), new UnaryNot(new And(new Wildcard(0), new Wildcard(1))));
        // a & !b == a \ b
        pushNotUpAndGenMinus.add(new And(new Wildcard(0), new UnaryNot(new Wildcard(1))), new Minus(new Wildcard(0), new Wildcard(1)));
        // a | !b == !( b \ a )
        pushNotUpAndGenMinus.add(new Or(new Wildcard(0), new UnaryNot(new Wildcard(1))), new UnaryNot(new Minus(new Wildcard(1), new Wildcard(0))));
        // a & ( prop == null ) == a \ ( prop != null )
        pushNotUpAndGenMinus.add(new And(new Wildcard(0), new PropertyEqualToPropertyNoNull(0)), new Minus(new Wildcard(0), new PropertyEqualToPropertyNoNull(0)));
        // a | ( prop == null ) == !( ( prop != null ) \ a )
        pushNotUpAndGenMinus.add(new Or(new Wildcard(0), new PropertyEqualToPropertyNoNull(0)), new UnaryNot(new Minus(new PropertyEqualToPropertyNoNull(0), new Wildcard(0))));
        // ( prop == null ) == !( prop != null )
        pushNotUpAndGenMinus.add(new PropertyEqualToPropertyNoNull(0), new UnaryNot(new PropertyEqualToPropertyNoNull(0)));
        // a & ( link == null ) == a \ ( link != null )
        pushNotUpAndGenMinus.add(new And(new Wildcard(0), new LinkEqualToLinkNotNull(0)), new Minus(new Wildcard(0), new LinkEqualToLinkNotNull(0)));
        // a | ( link == null ) == !( ( link != null ) \ a )
        pushNotUpAndGenMinus.add(new Or(new Wildcard(0), new LinkEqualToLinkNotNull(0)), new UnaryNot(new Minus(new LinkEqualToLinkNotNull(0), new Wildcard(0))));
        // ( link == null ) == !( link != null )
        pushNotUpAndGenMinus.add(new LinkEqualToLinkNotNull(0), new UnaryNot(new LinkEqualToLinkNotNull(0)));

        // !a == all \ a
        removeSingleNot.add(new UnaryNot(new Wildcard(0)), new Minus(NodeFactory.all(), new Wildcard(0)));
    }

    public OptimizationPlan(boolean a) {
        rules = new ArrayList<OptimizationRule>();
        applyOnEnter = a;
    }

    /**
     * Adds rules for converting {@code source} to {@code dest}.
     * A rule for every combination of flipped/unflipped commutative operations added.
     *
     * @param source source pattern
     * @param dest   destination pattern
     */
    @SuppressWarnings("ObjectAllocationInLoop")
    public void add(NodeBase source, NodeBase dest) {
        // amount of commutative nodes
        int n = 0;
        for (NodeBase node : source.getDescendants()) {
            if (node instanceof CommutativeOperator) {
                n++;
            }
        }
        for (int i = 0; i < 1 << n; i++) {
            Root root = new Root(source.getClone());
            int j = 0;
            for (NodeBase node : root.getDescendants()) {
                if (node instanceof CommutativeOperator) {
                    if ((i & 1 << j) != 0) {
                        ((CommutativeOperator) node).flipChildren();
                    }
                    j++;
                }
            }
            rules.add(new OptimizationRule(root.getChild(), dest));
        }
    }

}
