package jetbrains.exodus.console;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.mozilla.javascript.*;
import org.mozilla.javascript.tools.ToolErrorReporter;

import java.awt.event.KeyEvent;
import java.io.*;
import java.lang.reflect.UndeclaredThrowableException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 */
public class RhinoCommand implements Command, Runnable {

    private InputStreamReader in;
    private PrintStream out;
    private PrintStream err;
    private ExitCallback callback;
    private volatile boolean stopped = false;

    RhinoCommand() {
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

                //System.out.println((int)c);

                if (c == -1 || c == 3) return null;
                if (c == '\n' || c == '\r') {
                    println();
                    break;
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

    private boolean isPrintableChar( char c ) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of( c );
        return (!Character.isISOControl(c)) &&
                c != KeyEvent.CHAR_UNDEFINED &&
                block != null &&
                block != Character.UnicodeBlock.SPECIALS;
    }

    private void processInput(Context cx) {
        Scriptable scope = cx.initStandardObjects();

        println(cx.getImplementationVersion());
        println("Welcome to xodus console.");

        boolean hitEOF = false;
        while (!hitEOF) {
            out.flush();
            int lineno = 0;
            String source = "";

            // Collect lines of source to compile.
            while (true) {
                lineno++;
                String newline;
                print('>');
                newline = readLine();
                if (newline == null) {
                    hitEOF = true;
                    break;
                }
                source = source + newline + "\n";
                if (cx.stringIsCompilableUnit(source)) break;
            }
            try {
                long start = System.currentTimeMillis();
                Script script = cx.compileString(source, "<stdin>", lineno, null);
                if (script != null) {
                    Object result = script.exec(cx, scope);
                    // Avoid printing out undefined or function definitions.
                    if (result != Context.getUndefinedValue() && !(result instanceof Function && source.trim().startsWith("function"))) {
                        try {
                            println(Context.toString(result));
                        } catch (RhinoException rex) {
                            ToolErrorReporter.reportException(
                                    cx.getErrorReporter(), rex);
                        }
                    }
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
}
