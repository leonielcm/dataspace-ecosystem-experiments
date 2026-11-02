plugins {
    java
}

// Runtimes started by the test, each on its own classpath (EDC EmbeddedRuntime + printClasspath): they are not
// test dependencies, but they must be built before the test runs.
val runtimesUnderTest: Configuration by configurations.creating {
    isCanBeConsumed = false
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
    }
}

dependencies {
    runtimesUnderTest(project(":system-tests:mcp-e2e:control-plane"))
    runtimesUnderTest(project(":system-tests:mcp-e2e:data-plane"))

    testImplementation(libs.edc.core.junit)
    testImplementation(testFixtures(libs.edc.ext.api.management.test.fixtures))
    testImplementation(libs.edc.spi.core)
    testImplementation(libs.restAssured)
    testImplementation(libs.awaitility)
    testImplementation(libs.assertj)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.mcp.core)
    testImplementation(libs.mcp.json.jackson2)
    testImplementation(libs.jetty.ee10.servlet)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgres)
    // shared by every runtime: JDBC registers a driver only once per JVM
    testRuntimeOnly(libs.postgres)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    dependsOn(runtimesUnderTest)
    // opt-in test against a real MCP server, see McpOverDataspaceIntegrationTest
    listOf("mcp.real.url", "mcp.real.tool", "mcp.real.args").forEach { key ->
        System.getProperty(key)?.let { systemProperty(key, it) }
    }
}

edcBuild {
    publish.set(false)
}
