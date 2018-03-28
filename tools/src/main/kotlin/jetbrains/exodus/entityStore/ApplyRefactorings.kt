package jetbrains.exodus.entityStore

import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.Reflect
import java.io.File

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
        return
    }

    val dir = File(args[0])
    val name = if (args.size > 1) {
        args[1]
    } else {
        "teamsysstore"
    }
    PersistentEntityStoreImpl(Reflect.openEnvironment(dir, false), name).apply {
        close()
    }
}

internal fun printUsage() {
    println("Usage: Refactorings <environment path> [store name]")
    println("       default store name is teamsysstore")
}
