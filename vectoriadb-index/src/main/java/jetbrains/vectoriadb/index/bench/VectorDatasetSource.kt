/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.vectoriadb.index.bench

import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

interface VectorDatasetSource {
    fun downloadArchive(archiveName: String, targetDir: Path): File
}

object IrisaFrDatasetSource: VectorDatasetSource {
    override fun downloadArchive(archiveName: String, targetDir: Path): File {
        val benchArchivePath = targetDir.resolve(archiveName)

        if (Files.exists(benchArchivePath)) {
            println("$archiveName already exists in $targetDir")
            return benchArchivePath.toFile()
        }

        println("Downloading $archiveName from ftp.irisa.fr into $targetDir")

        with(FTPClient()) {
            connect("ftp.irisa.fr")
            enterLocalPassiveMode()

            val loggedIn = login("anonymous", "anonymous")
            check(loggedIn) { "Failed to login to ftp.irisa.fr" }
            println("Logged in to ftp.irisa.fr")

            setFileType(FTP.BINARY_FILE_TYPE)
            try {
                Files.newOutputStream(benchArchivePath).use { outputStream ->
                    retrieveFile("/local/texmex/corpus/$archiveName", outputStream)
                }
            } finally {
                logout()
                disconnect()
            }

            println("$archiveName downloaded")
        }
        return benchArchivePath.toFile()
    }
}