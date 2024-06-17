dependencies {
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-utils"))
    implementation("com.orientechnologies:orientdb-core:4.0.0-SNAPSHOT")
    api(project(":xodus-openAPI"))
    implementation("com.github.penemue:keap:0.3.0")
    implementation(project(":xodus-environment"))
    implementation(libs.slf4j.simple)

    testImplementation(project(":xodus-utils", "testArtifacts"))
    testImplementation(project(":xodus-entity-store", "testArtifacts"))
    testImplementation(libs.mockk)
    testImplementation(libs.truth)
    testImplementation(kotlin("test"))
}

tasks {
    register<JavaExec>("migrateXodusToOrient") {
        group = "application"
        mainClass = "jetbrains.exodus.query.metadata.MigrateXodusToOrientKt"
        classpath = sourceSets["main"].runtimeClasspath
        jvmArgs = listOf(
            "-server",
            "-Xmx16g",
            "-XX:+HeapDumpOnOutOfMemoryError",
        )
        systemProperties = mapOf(
            "xodusDatabaseDirectory" to (project.findProperty("xodusDatabaseDirectory")),
            "xodusStoreName" to (project.findProperty("xodusStoreName")),
            "xodusCipherKey" to (project.findProperty("xodusCipherKey")),
            "xodusCipherIV" to (project.findProperty("xodusCipherIV")),

            "orientDatabaseType" to (project.findProperty("orientDatabaseType")),
            "orientDatabaseDirectory" to (project.findProperty("orientDatabaseDirectory")),
            "orientDatabaseName" to (project.findProperty("orientDatabaseName")),
            "orientUsername" to (project.findProperty("orientUsername")),
            "orientPassword" to (project.findProperty("orientPassword")),

            "validateDataAfterMigration" to (project.findProperty("validateDataAfterMigration")),
        )
    }
}