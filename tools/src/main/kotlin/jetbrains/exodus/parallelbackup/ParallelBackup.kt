/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.parallelbackup

import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.env.Environments
import jetbrains.exodus.util.CompressBackupUtil
import java.io.File
import java.nio.file.Paths


fun parallelBackup(args: Array<String>) {
    if (args.size < 2) {
        println("Paths to the database and backup directory should be provided.")
    }


    val dbPath = args[0]
    val backupDirPath = args[1]


    val storeName = if (args.size == 2) {
        "teamsysstore"
    } else {
        args[2]
    }

    println("Starting of backup of database located at $dbPath with store name $storeName to the directory $backupDirPath.")
    val start = System.currentTimeMillis()
    val backupPath = Environments.newInstance(dbPath).use {
        PersistentEntityStores.newInstance(it, storeName).use { store ->
            CompressBackupUtil.parallelBackup(store, File(backupDirPath), null)
        }
    }
    val end = System.currentTimeMillis()

    println(
        "Backup of  database located at $dbPath with store name " +
                "$storeName to the directory $backupDirPath is completed."
    )
    println("Backup file is located at ${backupPath.absolutePath}")
    println("Backup execution time is ${(end - start) / (1_000 * 60)} minutes.")
    println("Do not forget to post-process backup after unpacking.")
}

fun parallelBackupPostProcessing(args: Array<String>) {
    if (args.isEmpty()) {
        println("Location of backup directory should be provided.")
    }

    val backupPath = Paths.get(args[0]).toAbsolutePath()
    println("Starting of post-processing of database backup located at $backupPath")
    val start = System.currentTimeMillis()
    CompressBackupUtil.postProcessBackup(backupPath)
    val end = System.currentTimeMillis()

    println("Post-processing of database backup located at $backupPath is completed")
    println("Postprocessing took  ${(end - start) / (1_000 * 60)} minutes.")
    println("Backup is ready for use.")
}