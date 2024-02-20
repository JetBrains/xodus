plugins {
    id("com.github.johnrengelman.shadow")
}

dependencies {
    implementation(project(":xodus-crypto"))
    implementation(project(":xodus-environment"))
    implementation(project(":xodus-entity-store"))
    implementation(project(":xodus-compress"))
    api(project(":xodus-openAPI"))
    implementation(project(":xodus-utils"))
    implementation("org.slf4j:slf4j-jdk14:2.0.7")
}

val testArtifacts: Configuration by configurations.creating

tasks {
    shadowJar {
        manifest {
            attributes["Main-Class"] = "jetbrains.exodus.MainKt"
        }
    }

    jar {
        enabled = false
    }

    val jarTest by creating(Jar::class) {
        archiveClassifier.set("test")
        from(sourceSets.test.get().output)
    }

    artifacts {
        add("default", shadowJar)
        add("testArtifacts", jarTest)
    }

    build {
        dependsOn(shadowJar)
    }
}