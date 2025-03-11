plugins {
    id("java")
    kotlin("jvm") version "2.1.10"
}

group = "woow"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.exposed:exposed-core:0.41.1")
    implementation("org.jetbrains.exposed:exposed-dao:0.41.1")
    implementation("org.jetbrains.exposed:exposed-jdbc:0.41.1")
    implementation("io.r2dbc:r2dbc-spi:1.0.0.RELEASE")
    implementation("org.postgresql:r2dbc-postgresql:1.0.7.RELEASE")
    implementation("io.r2dbc:r2dbc-pool:1.0.0.RELEASE")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.6.4")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}