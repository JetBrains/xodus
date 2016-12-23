/*
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
function prepareOptimizationPlanDSL() {
    queries = Packages.jetbrains.exodus.query;
    plan = queries.OptimizationPlan;
    tkei = queries.TreeKeepingEntityIterable;
    prepare = plan.PLANS.get(0);
    optimize = plan.PLANS.get(1);
    pushNotUpAndGenMinus = plan.PLANS.get(2);
    removeSingleNot = plan.PLANS.get(3);

    ALL = plan.ALL;
    PE2PNN = plan.PE2PNN;
    LE2LNN = plan.LE2LNN;
    MPR = plan.MPR;

    not = plan.not;
    or = plan.or;
    minus = plan.minus;
    and = plan.and;

    A = plan.wildcard(0);
    B = plan.wildcard(1);
    C = plan.wildcard(2);
    D = plan.wildcard(3);
}

function resetOptimizations() {
    Packages.jetbrains.exodus.query.OptimizationPlan.resetAll();
    prepareOptimizationPlanDSL();
}

function initOptimizations() {
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

function optimizeTree(tree) {
    var sorts = queries.Sorts();
    var root = queries.Root(tree.getClone());
    for (var iterator = plan.PLANS.iterator(); iterator.hasNext();) {
        root.optimize(sorts, iterator.next());
    }
    root.cleanSorts(sorts);
    return root.getChild();
}
