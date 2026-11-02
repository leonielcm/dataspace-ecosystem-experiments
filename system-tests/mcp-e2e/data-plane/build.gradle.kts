plugins {
    `java-library`
}

// Data plane runtime of the MCP end-to-end test: the project's data plane launcher, plus the SQL stack its telemetry
// record store needs (data consumption metrics), against a test Postgres with schema auto-creation.
dependencies {
    runtimeOnly(project(":launchers:data-plane:data-plane-base"))
    runtimeOnly(libs.bundles.coresql)
    runtimeOnly(libs.edc.sql.bootstrapper)
    runtimeOnly(libs.edc.sql.lease.core)
    runtimeOnly(libs.edc.transaction.local)
}

edcBuild {
    publish.set(false)
}
