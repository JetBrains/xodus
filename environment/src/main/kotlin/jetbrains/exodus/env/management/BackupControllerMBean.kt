package jetbrains.exodus.env.management

const val BACKUP_CONTROLLER_NAME_PREFIX = "jetbrains.exodus.env: type=BackupController"

interface BackupControllerMBean {
    fun prepareBackup() : Array<String>
    fun finishBackup()
}