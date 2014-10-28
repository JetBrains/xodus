package jetbrains.exodus.console;

import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;

/**
 *
 */
public class RhinoCommandFactory implements Factory<Command> {

    @Override
    public Command create() {
        return new RhinoCommand();
    }
}
