package jetbrains.exodus.sshd;

import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 *
 */
public class RhinoCommandFactory implements Factory<Command> {

    @NotNull
    private Map<String, Object> config;

    RhinoCommandFactory(@NotNull Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public Command create() {
        return new RhinoCommand(config);
    }
}
