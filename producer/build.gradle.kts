plugins {
    id("java")
}

group = "org.qubits"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.kafka:kafka-clients:3.5.1")
    implementation("ch.qos.logback:logback-core:1.4.8")
    implementation("ch.qos.logback:logback-classic:1.4.8")
    compileOnly("org.projectlombok:lombok:1.18.28")
    annotationProcessor("org.projectlombok:lombok:1.18.28")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.1")
}

tasks.test {
    useJUnitPlatform()
}