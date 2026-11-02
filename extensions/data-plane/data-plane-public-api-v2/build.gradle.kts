/*
 *  Copyright (c) 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - improvements
 *       Mercedes-Benz Tech Innovation GmbH - publish public api context into dedicated swagger hub page
 *
 */


plugins {
    `java-library`
    id(libs.plugins.swagger.get().pluginId)
}

dependencies {
    api(libs.edc.spi.http)
    api(libs.edc.spi.web)
    api(libs.edc.spi.dpf)
    api(libs.edc.spi.edrstore)
    api(libs.edc.spi.dpf.http)

    implementation(libs.edc.core.jersey)
    implementation(libs.edc.lib.util)
    implementation(libs.edc.ext.dpf.util)
    implementation(libs.edc.ext.dpf.http)
    implementation(libs.jakarta.rsApi)

    testImplementation(libs.edc.core.junit)
    testImplementation(libs.jersey.multipart)
    testImplementation(libs.restAssured)
    testImplementation(libs.mockserver.netty)
    testImplementation(libs.mockserver.client)
    testImplementation(testFixtures(libs.edc.core.jersey))
}
edcBuild {
    swagger {
        apiGroup.set("public-api")
    }
}


