plugins {
    id("java-library")
}

group = "io.richard.event"
version = "0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":annotations"))
    implementation("org.slf4j:slf4j-api:2.0.3")

    implementation("com.fasterxml.jackson.core:jackson-core:2.14.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.0")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.14.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.14.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.14.0")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.0")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}