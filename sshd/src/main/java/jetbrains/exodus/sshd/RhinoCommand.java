package jetbrains.exodus.sshd;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.entitystore.PersistentEntityStore;
import jetbrains.exodus.entitystore.StoreTransaction;
import jetbrains.exodus.entitystore.StoreTransactionalExecutable;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.jetbrains.annotations.NotNull;
import org.mozilla.javascript.*;
import org.mozilla.javascript.tools.ToolErrorReporter;

import java.awt.event.KeyEvent;
import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class RhinoCommand implements Command, Runnable {

    private static final String INITIAL_SCRIPT = "default.js";
    private InputStreamReader in;
    private PrintStream out;
    private PrintStream err;
    private ExitCallback callback;
    private volatile boolean stopped = false;

    @NotNull
    private PersistentEntityStore entityStore;

    RhinoCommand(@NotNull PersistentEntityStore entityStore) {
        this.entityStore = entityStore;
    }

    @Override
    public void setInputStream(InputStream in) {
        try {
            this.in = new InputStreamReader(in, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setOutputStream(OutputStream out) {
        this.out = new PrintStream(out);
    }

    @Override
    public void setErrorStream(OutputStream err) {
        this.err = new PrintStream(err);
    }

    @Override
    public void setExitCallback(ExitCallback callback) {
        this.callback = callback;
    }

    @Override
    public void start(Environment env) throws IOException {
        new Thread(this).start();
    }

    @Override
    public void destroy() {
        stopped = true;
    }

    @Override
    public void run() {
        Context cx = Context.enter();
        cx.setClassShutter(new ClassShutterImpl());
        cx.setErrorReporter(new ErrorReporterImpl());
        try {
            processInput(cx);
        } finally {
            Context.exit();
            callback.onExit(0);
        }
    }

    private void println(String s) {
        out.print(s + "\n\r");
        out.flush();
    }

    private void println() {
        out.print("\n\r");
        out.flush();
    }

    private void print(char s) {
        out.print(s);
        out.flush();
    }

    private String readLine() {
        StringBuilder s = new StringBuilder();

        try {
            while (true) {
                char c = (char) in.read();

                if (c == -1 || c == 3) return null;
                if (c == '\n' || c == '\r') {
                    println();
                    break;
                }
                // delete
                if (c == 127 && s.length() > 0) {
                    s.deleteCharAt(s.length() - 1);
                    print('\b');
                }

                if (!isPrintableChar(c)) continue;

                print(c);
                s.append(c);
            }
            return s.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);
        return (!Character.isISOControl(c)) &&
                c != KeyEvent.CHAR_UNDEFINED &&
                block != null &&
                block != Character.UnicodeBlock.SPECIALS;
    }

    private Scriptable createScope(Context cx) {
        final Scriptable scope = cx.initStandardObjects(null, true);
        ScriptableObject.putProperty(scope, "store", Context.javaToJS(entityStore, scope));
        ScriptableObject.putProperty(scope, "out", Context.javaToJS(out, scope));
        try {
            cx.evaluateReader(scope, new InputStreamReader(this.getClass().getResourceAsStream(INITIAL_SCRIPT)), INITIAL_SCRIPT, 1, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return scope;
    }

    private void processInput(final Context cx) {
        final Scriptable scope = createScope(cx);

        println(cx.getImplementationVersion());
        println("Welcome to xodus console. To exit press Ctrl-C.");

        boolean hitEOF = false;
        while (true) {
            out.flush();
            int lineno = 0;
            final StringBuilder source = new StringBuilder();

            // Collect lines of source to compile.
            while (true) {
                lineno++;
                print(lineno == 1 ? '>' : ' ');
                String newline = readLine();
                if (newline == null) return;
                source.append(newline).append("\n");
                if (cx.stringIsCompilableUnit(source.toString())) break;
            }

            // execute script in transaction
            try {
                final Script script = cx.compileString(source.toString(), "<stdin>", lineno, null);
                if (script != null) {
                    long start = System.currentTimeMillis();

                    entityStore.executeInTransaction(new StoreTransactionalExecutable() {
                        @Override
                        public void execute(@NotNull StoreTransaction txn) {
                            ScriptableObject.putProperty(scope, "txn", Context.javaToJS(txn, scope));
                            Object result = script.exec(cx, scope);
                            if (result != Context.getUndefinedValue()) {
                                println(Context.toString(result));
                            }
                        }
                    });

                    println("Complete in " + ((System.currentTimeMillis() - start)) + " ms");
                }
            } catch (RhinoException rex) {
                ToolErrorReporter.reportException(cx.getErrorReporter(), rex);
            } catch (VirtualMachineError ex) {
                // Treat StackOverflow and OutOfMemory as runtime errors
                ex.printStackTrace();
                String msg = ToolErrorReporter.getMessage("msg.uncaughtJSException", ex.toString());
                Context.reportError(msg);
            }
        }
    }

    private class ErrorReporterImpl implements ErrorReporter {
        @Override
        public void error(String message, String sourceName, int line, String lineSource, int lineOffset) {
            StringBuilder s = new StringBuilder();
            if (line > 0) {
                s.append(line).append(": ");
            }
            s.append(message);
            println(s.toString());
        }

        @Override
        public EvaluatorException runtimeError(String message, String sourceName, int line, String lineSource, int lineOffset) {
            return new EvaluatorException(message, sourceName, line, lineSource, lineOffset);
        }

        @Override
        public void warning(String message, String sourceName, int line, String lineSource, int lineOffset) {
        }
    }

    private static Set<String> FORBIDDEN_CLASSES = new HashSet<String>() {{
        add("java.lang.System");
    }};

    private class ClassShutterImpl implements ClassShutter {

        @Override
        public boolean visibleToScripts(String fullClassName) {
            return !FORBIDDEN_CLASSES.contains(fullClassName);
        }
    }
}
