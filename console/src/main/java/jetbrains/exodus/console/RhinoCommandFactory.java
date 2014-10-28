package jetbrains.exodus.console;

import jetbrains.exodus.entitystore.PersistentEntityStore;
import org.apache.sshd.common.Factory;
import org.apache.sshd.server.Command;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class RhinoCommandFactory implements Factory<Command> {

    @NotNull
    private PersistentEntityStore store;

    RhinoCommandFactory(@NotNull PersistentEntityStore store) {
        this.store = store;
    }

    @Override
    public Command create() {
        return new RhinoCommand(store);
    }
}
