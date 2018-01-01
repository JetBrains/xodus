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
package jetbrains.exodus.query;


import java.util.ArrayList;
import java.util.List;

public class OptimizationPlan {
    public static final NodeBase ALL = NodeFactory.all();
    public static final NodeBase PE2PNN = new PropertyEqualToPropertyNoNull(0);
    public static final NodeBase LE2LNN = new LinkEqualToLinkNotNull(0);
    public static final NodeBase MPR = new MergePropertyRanges(0);

    private static final NodeBase A = wildcard(0);
    private static final NodeBase B = wildcard(1);
    private static final NodeBase C = wildcard(2);
    private static final NodeBase D = wildcard(3);

    public static final List<OptimizationPlan> PLANS;

    /* package */ final List<OptimizationRule> rules;
    /**
     * true means that rules from this plan will be applied on enter during depth-first search of the QueryTree
     */
    public final boolean applyOnEnter;

    static {
        final OptimizationPlan prepare = new OptimizationPlan(false);
        final OptimizationPlan optimize = new OptimizationPlan(false);
        final OptimizationPlan pushNotUpAndGenMinus = new OptimizationPlan(false);
        final OptimizationPlan removeSingleNot = new OptimizationPlan(false);

        final List<OptimizationPlan> plans = new ArrayList<>(4);
        plans.add(prepare);
        plans.add(optimize);
        plans.add(pushNotUpAndGenMinus);
        plans.add(removeSingleNot);
        PLANS = plans;

        // a \ b == a & !b
        prepare.add(minus(A, B), and(A, not(B)));

        // ( a | b ) & ( a | c ) == a | ( b & c )
        optimize.add(and(or(A, B), or(A, C)), or(A, and(B, C)));
        // ( a & b ) | ( a & c ) == a & ( b | c )
        optimize.add(or(and(A, B), and(A, C)), and(A, or(B, C)));
        // all & a == a
        optimize.add(and(ALL, A), A);
        // all | a == all
        optimize.add(or(ALL, A), ALL);
        // a & !( a & !b ) == a & b
        // needed for optimizing a \ ( a \ b ) which is a & b
        optimize.add(and(A, not(and(A, not(B)))), and(A, B));
        // for optimizing #Unassigned #Unresolved when no project is selected (if some assignee bundles differ)
        // ( ( a & b ) | c ) | ( d & b ) == ( ( a | d ) & b ) | c
        optimize.add(or(or(and(A, B), C), and(D, B)), or(and(or(A, D), B), C));
        // ( a <= x <= b && c <= x <= d ) == ( max( a, c ) <= x <= min( b, d ) )
        optimize.add(MPR, MPR);

        // !!a == a
        pushNotUpAndGenMinus.add(not(not(A)), A);
        // !a & !b == !( a | b )
        pushNotUpAndGenMinus.add(and(not(A), not(B)), not(or(A, B)));
        // !a | !b == !( a & b )
        pushNotUpAndGenMinus.add(or(not(A), not(B)), not(and(A, B)));
        // a & !b == a \ b
        pushNotUpAndGenMinus.add(and(A, not(B)), minus(A, B));
        // a | !b == !( b \ a )
        pushNotUpAndGenMinus.add(or(A, not(B)), not(minus(B, A)));
        // (a \ b) \ b == a \ b
        pushNotUpAndGenMinus.add(minus(minus(A, B), B), minus(A, B));
        // a & ( prop == null ) == a \ ( prop != null )
        pushNotUpAndGenMinus.add(and(A, PE2PNN), minus(A, PE2PNN));
        // a | ( prop == null ) == !( ( prop != null ) \ a )
        pushNotUpAndGenMinus.add(or(A, PE2PNN), not(minus(PE2PNN, A)));
        // ( prop == null ) == !( prop != null )
        pushNotUpAndGenMinus.add(PE2PNN, not(PE2PNN));
        // a & ( link == null ) == a \ ( link != null )
        pushNotUpAndGenMinus.add(and(A, LE2LNN), minus(A, LE2LNN));
        // a | ( link == null ) == !( ( link != null ) \ a )
        pushNotUpAndGenMinus.add(or(A, LE2LNN), not(minus(LE2LNN, A)));
        // ( link == null ) == !( link != null )
        pushNotUpAndGenMinus.add(LE2LNN, not(LE2LNN));

        // !a == all \ a
        removeSingleNot.add(not(A), minus(ALL, A));
    }

    public static UnaryNot not(NodeBase child) {
        return new UnaryNot(child);
    }

    public static Or or(NodeBase left, NodeBase right) {
        return new Or(left, right);
    }

    public static Minus minus(NodeBase left, NodeBase right) {
        return new Minus(left, right);
    }

    public static And and(NodeBase left, NodeBase right) {
        return new And(left, right);
    }

    public static Wildcard wildcard(int t) {
        return new Wildcard(t);
    }

    public OptimizationPlan(boolean a) {
        rules = new ArrayList<>();
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

    public static void resetAll() {
        for (int i = 0; i < PLANS.size(); i++) {
            PLANS.set(i, new OptimizationPlan(false));
        }
    }

}
