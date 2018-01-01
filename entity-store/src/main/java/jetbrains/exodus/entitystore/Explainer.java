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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"HardcodedLineSeparator"})
public class Explainer {

    public static final String INSTANTIATED_FROM = "instantiated from";

    public static final String CURSOR_ADVANCES = "cursor advances";

    public static final String CURSOR_ADVANCES_BY_TYPE = "cursor advances by type";

    public static final String _CURSOR_ADVANCES_BY_TYPE = "#cursor advances by type";

    public static final String CURSOR_ADVANCES_BY_HANDLE = "cursor advances by handle";

    public static final String _CURSOR_ADVANCES_BY_HANDLE = "#cursor advances by handle";

    public static final String CURSOR_ADVANCES_FOR_FIRST = "cursor advances for first";

    public static final String AVERAGE_CURSOR_ADVANCES = "average cursor advances";

    public static final String ITERABLE_ADVANCES = "iterable advances";

    public static final String INITIAL_TREE = "initial tree";

    public static final String OPTIMIZED_TREE = "optimized tree";

    public static final String CONCURRENT_TRAVERSE_WARNING = "WARNING: concurrent traverse of single iterable";

    private static final String PACKAGE_TO_SKIP_IN_STACKTRACE = "jetbrains.teamsys.dnq.runtime.queries";

    private static final Logger logger = LoggerFactory.getLogger(Explainer.class);

    private final Map<Object, Map<String, Object>> queries = new ConcurrentHashMap<>();

    private static final Collection<String> PERFORMANCE_PARAMETERS = new ArrayList<>();

    private static final Map<String, Double> WORST_VALUES = new HashMap<>();

    private static Thread forceExplainThread = null;

    private boolean explainOn;

    static {
        PERFORMANCE_PARAMETERS.add(CURSOR_ADVANCES_FOR_FIRST);
        PERFORMANCE_PARAMETERS.add(AVERAGE_CURSOR_ADVANCES);

        for (String parameter : PERFORMANCE_PARAMETERS) {
            WORST_VALUES.put(parameter, Double.MIN_VALUE);
        }
    }

    public Explainer(boolean isExplainOn) {
        this.explainOn = isExplainOn;
    }

    public boolean isExplainOn() {
        return explainOn || Explainer.isExplainForcedForThread();
    }

    public Object genOrigin() {
        if (Explainer.isExplainForcedForThread()) {
            return Thread.currentThread();
        } else if (explainOn) {
            return new Throwable();
        }
        return null;
    }

    public static void forceExplain(Thread thread) {
        forceExplainThread = thread;
    }

    public static boolean isExplainForcedForThread() {
        return Thread.currentThread() == forceExplainThread;
    }

    public void start(Object origin) {
        if (origin != null) {
            if (queries.get(origin) == null) {
                Map<String, Object> map = new ConcurrentHashMap<>();
                queries.put(origin, map);
                map.put(INSTANTIATED_FROM, "\nat " + stripStackTrace(new Throwable()));
            } else {
                queries.get(origin).put(CONCURRENT_TRAVERSE_WARNING, "");
            }
        }
    }

    public void append(Object origin, String parameter, Object value) {
        Map<String, Object> query = queries.get(origin);
        if (query != null) {
            Object o = query.get(parameter);
            query.put(parameter, o == null ? value : o + ", " + value);
        }
    }

    public void explain(Object origin, String parameter, Object value) {
        if (origin != null) {
            Map<String, Object> query = queries.get(origin);
            if (query != null) {
                query.put(parameter, value);
            }
        }
    }

    public void explain(Object origin, String parameter) {
        if (origin != null) {
            Map<String, Object> query = queries.get(origin);
            if (query != null) {
                Object value = query.get(parameter);
                if (value == null) {
                    value = 0;
                }
                query.put(parameter, (Integer) value + 1);
                if (ITERABLE_ADVANCES.equals(parameter)) {
                    if ((Integer) query.get(ITERABLE_ADVANCES) == 1 && query.get(CURSOR_ADVANCES) != null) {
                        query.put(CURSOR_ADVANCES_FOR_FIRST, query.get(CURSOR_ADVANCES));
                    }
                }
            }
        }
    }

    public Map<String, Object> getParameters(Object origin) {
        return queries.get(origin);
    }

    @SuppressWarnings({"ObjectAllocationInLoop"})
    public void log(Object origin) {
        if (origin == null) {
            return;
        }
        Map<String, Object> query = queries.get(origin);
        if (query == null || query.get(ITERABLE_ADVANCES) == null) {
            return;
        }
        Object cursorAdvances = query.get(CURSOR_ADVANCES);
        Object cursorAdvancesForFirst = query.get(CURSOR_ADVANCES_FOR_FIRST);
        if (cursorAdvances != null && cursorAdvancesForFirst != null) {
            // not ITERABLE_ADVANCES - 1 because of some advances for the last unsuccessful hasNext
            query.put(AVERAGE_CURSOR_ADVANCES,
                    ((Integer) cursorAdvances - (Integer) cursorAdvancesForFirst) * 1.0 / (Integer) query.get(ITERABLE_ADVANCES));
        }

        List<Pair<Integer, String>> byType = new ArrayList<>();
        List<Pair<Integer, String>> byHandle = new ArrayList<>();
        Collection<String> toRemove = new HashSet<>();
        for (String parameter : query.keySet()) {
            if (parameter.startsWith(_CURSOR_ADVANCES_BY_TYPE)) {
                toRemove.add(parameter);
                byType.add(new Pair<>((Integer) query.get(parameter),
                        EntityIterableType.valueOf(parameter.substring(_CURSOR_ADVANCES_BY_TYPE.length() + 1)).toString()));
            }
            if (parameter.startsWith(_CURSOR_ADVANCES_BY_HANDLE)) {
                toRemove.add(parameter);
                byHandle.add(new Pair<>((Integer) query.get(parameter),
                        parameter.substring(_CURSOR_ADVANCES_BY_HANDLE.length() + 1)));
            }
        }
        for (String parameter : toRemove) {
            query.remove(parameter);
        }
        Comparator<Pair<Integer, String>> pairComparator = new Comparator<Pair<Integer, String>>() {
            @Override
            public int compare(Pair<Integer, String> p1, Pair<Integer, String> p2) {
                return p2.getFirst().compareTo(p1.getFirst());
            }
        };
        Collections.sort(byType, pairComparator);
        Collections.sort(byHandle, pairComparator);
        StringBuilder advancesByType = new StringBuilder();
        for (Pair<Integer, String> pair : byType) {
            advancesByType.append('\n').append(pair.getSecond()).append(": ").append(pair.getFirst());
        }
        StringBuilder advancesByHandle = new StringBuilder();
        for (Pair<Integer, String> pair : byHandle) {
            advancesByHandle.append('\n').append(pair.getFirst()).append(": ").append(pair.getSecond());
        }
        query.put(CURSOR_ADVANCES_BY_TYPE, advancesByType.toString());
        query.put(CURSOR_ADVANCES_BY_HANDLE, advancesByHandle.toString());

        boolean show = false;
        for (String parameter : PERFORMANCE_PARAMETERS) {
            if (query.get(parameter) == null) {
                continue;
            }
            double value = ((Number) query.get(parameter)).doubleValue();
            Double worst = WORST_VALUES.get(parameter);
            if (worst <= value * 2) {
                show = true;
                WORST_VALUES.put(parameter, Math.max(value, worst));
            }
        }

        if (show) {
            final StringBuilder stringWriter = new StringBuilder();
            stringWriter.append("Explain\n");
            for (Map.Entry<String, Object> entry : query.entrySet()) {
                stringWriter.append(entry.getKey());
                stringWriter.append(": ");
                stringWriter.append(entry.getValue().toString());
                stringWriter.append('\n');
            }
            logger.info(stringWriter.toString());
        }

        if (!isExplainForcedForThread()) {
            clean(origin);
        }
    }

    public void clean(Object origin) {
        if (origin != null) {
            queries.remove(origin);
        }
    }

    public static String stripStackTrace(Throwable throwable) {
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        int i = 0;
        while (i < stackTrace.length && !stackTrace[i].getClassName().startsWith(PACKAGE_TO_SKIP_IN_STACKTRACE)) {
            i++;
        }
        while (i < stackTrace.length && stackTrace[i].getClassName().startsWith(PACKAGE_TO_SKIP_IN_STACKTRACE)) {
            i++;
        }
        if (i >= stackTrace.length) {
            i = 0;
        }
        return stackTrace[i].toString();
    }
}
