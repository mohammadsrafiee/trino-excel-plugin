import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("java")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.trino"
version = "1.0-SNAPSHOT"

val jacksonCoreVersion = "2.19.0"
val apachePoiVersion = "5.4.1"
val testcontainersVersion = "1.19.7"
val assertjVersion = "3.25.3"
val mockitoVersion = "5.11.0"
val trinoVersion = "475"
val junitVersion = "5.10.2"
val airliftLogVersion = "332"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")
    compileOnly("io.trino:trino-memory:$trinoVersion")
    compileOnly("io.trino:trino-testing-containers:$trinoVersion")
    compileOnly("io.trino:trino-testing:$trinoVersion")
    compileOnly("io.trino:trino-tpch:$trinoVersion")

    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonCoreVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonCoreVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonCoreVersion")
    implementation("io.airlift:log:${airliftLogVersion}")
    implementation("io.airlift:bootstrap:${airliftLogVersion}")
    implementation("org.apache.poi:poi-ooxml:$apachePoiVersion")
    implementation("org.apache.poi:poi:$apachePoiVersion")

    testImplementation("io.trino:trino-jdbc:$trinoVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:nginx")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:trino")
    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation(platform("org.testcontainers:testcontainers-bom:$testcontainersVersion"))

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    manifest {
        attributes("Implementation-Title" to "Trino Excel Plugin (Thin)")
        attributes("Implementation-Version" to project.version)
    }
}

tasks.named<ShadowJar>("shadowJar") {
    archiveBaseName.set(project.name)
    archiveClassifier.set("")
    archiveVersion.set(project.version.toString())
    mergeServiceFiles()
    isZip64 = true

    manifest {
        attributes("Implementation-Title" to "Trino Excel Plugin (Fat)")
        attributes("Implementation-Version" to project.version)
    }

    dependencies {
//        exclude(dependency("io.trino:trino-spi"))
        exclude(dependency("io.trino:trino-memory"))
        exclude(dependency("io.trino:trino-testing-containers"))
        exclude(dependency("io.trino:trino-testing"))
        exclude(dependency("io.trino:trino-tpch"))
    }
}

tasks.test {
    useJUnitPlatform()
    dependsOn(tasks.jar)
    dependsOn(tasks.shadowJar)
}
