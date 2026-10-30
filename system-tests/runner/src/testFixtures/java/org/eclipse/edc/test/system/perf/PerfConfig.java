package org.eclipse.edc.test.system.perf;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Settings of the connector performance experiment, read from {@code perf.*} system properties
 * (forwarded to the test JVM by system-tests/runner/build.gradle.kts).
 *
 * <p>Two targets:
 * <ul>
 *     <li>{@code playground} (default): the Lourmel VM. The experiment seeds its own asset on the
 *     provider, reaches provider-backend and the provider data plane through kubectl port-forward,
 *     and measures every path at every payload size.</li>
 *     <li>{@code uat}: an existing environment whose provider and backend are not ours. Only our
 *     consumer connector is used, on an existing asset; "direct" is the real API URL. No seeding,
 *     no port-forward, no provider public API path, no payload sizes.</li>
 * </ul>
 */
public record PerfConfig(
        Target target,
        int warmup,
        int runs,
        int coldRuns,
        int rejectionRuns,
        List<Long> sizes,
        Path outputDir,
        String protocol,
        boolean retirementPhase,
        Playground playground,
        Uat uat) {

    public enum Target { PLAYGROUND, UAT }

    /**
     * Playground settings: where provider-backend and the provider data plane are.
     *
     * @param providerNamespace         namespace of the provider participant
     * @param backendService            kubectl resource of provider-backend, e.g. svc/provider-backend
     * @param backendPort               port of that service
     * @param providerPublicMode        how the provider public API is reached: portforward, ingress or disabled
     * @param dataPlaneService          kubectl resource of the provider data plane
     * @param dataPlanePublicPort       public API port of that service
     * @param providerPublicIngressUrl  base URL of the provider public API through the ingress
     */
    public record Playground(String providerNamespace, String backendService, int backendPort,
                             String providerPublicMode, String dataPlaneService, int dataPlanePublicPort,
                             String providerPublicIngressUrl) {
    }

    /**
     * UAT settings: our consumer connector, the existing provider asset, and the real API.
     *
     * @param consumerName          consumer connector name, only used in logs and reports
     * @param consumerDid           consumer DID, optional: only used to identify the client
     * @param consumerManagementUrl e.g. https://host/consumer/cp/management
     * @param consumerDataUrl       e.g. https://host/consumer/dp/data
     * @param apiKey                x-api-key sent to the consumer management and data APIs, blank for none
     * @param providerDid           provider DID (counterPartyId)
     * @param providerDspUrl        provider DSP endpoint (counterPartyAddress)
     * @param assetId               existing asset to negotiate
     * @param connectorQuery        query string appended to the consumer data API URL, e.g. q=paris
     * @param directUrl             real API URL called for the "direct" path
     * @param directHeaders         headers of the direct call, e.g. the provider API key
     */
    public record Uat(String consumerName, String consumerDid, String consumerManagementUrl, String consumerDataUrl,
                      String apiKey, String providerDid, String providerDspUrl, String assetId,
                      String connectorQuery, String directUrl, Map<String, String> directHeaders) {

        /**
         * Without the API key and the direct call header values: this ends up in logs and reports.
         */
        @Override
        public String toString() {
            return "Uat[consumer=%s (%s), management=%s, data=%s, apiKey=%s, provider=%s at %s, asset=%s, query=%s, direct=%s, directHeaders=%s]"
                    .formatted(consumerName, consumerDid, consumerManagementUrl, consumerDataUrl, apiKey.isBlank() ? "none" : "***",
                            providerDid, providerDspUrl, assetId, connectorQuery, directUrl, directHeaders.keySet());
        }
    }

    public static PerfConfig fromSystemProperties(String developerNamespace, String clusterHostname) {
        return from(System::getProperty, developerNamespace, clusterHostname);
    }

    static PerfConfig from(Function<String, String> properties, String developerNamespace, String clusterHostname) {
        var p = new Props(properties);
        var target = Target.valueOf(p.get("perf.target", "playground").toUpperCase(Locale.ROOT));
        var providerNamespace = developerNamespace.isBlank() ? "dse-provider" : developerNamespace + "-dse-provider";

        var playground = new Playground(
                p.get("perf.provider.namespace", providerNamespace),
                p.get("perf.backend.service", "svc/provider-backend"),
                p.getInt("perf.backend.port", 8080),
                p.get("perf.provider.public.mode", "portforward").toLowerCase(Locale.ROOT),
                p.get("perf.provider.dataplane.service", "svc/provider-dataplane"),
                p.getInt("perf.provider.dataplane.publicPort", 8181),
                p.get("perf.provider.public.ingressUrl", "http://%s/provider/dp/public".formatted(clusterHostname)));

        var uat = new Uat(
                p.get("perf.uat.consumer.name", "consumer"),
                p.get("perf.uat.consumer.did", "consumer"),
                p.get("perf.uat.consumer.managementUrl", ""),
                p.get("perf.uat.consumer.dataUrl", ""),
                p.get("perf.uat.apiKey", ""),
                p.get("perf.uat.provider.did", ""),
                p.get("perf.uat.provider.dspUrl", ""),
                p.get("perf.uat.assetId", ""),
                p.get("perf.uat.connector.query", ""),
                p.get("perf.uat.direct.url", ""),
                parseHeaders(p.get("perf.uat.direct.headers", "")));

        if (target == Target.UAT) {
            requireNonBlank(Map.of(
                    "perf.uat.consumer.managementUrl", uat.consumerManagementUrl(),
                    "perf.uat.consumer.dataUrl", uat.consumerDataUrl(),
                    "perf.uat.provider.did", uat.providerDid(),
                    "perf.uat.provider.dspUrl", uat.providerDspUrl(),
                    "perf.uat.assetId", uat.assetId(),
                    "perf.uat.direct.url", uat.directUrl()));
        }

        var runs = p.getInt("perf.runs", 200);
        return new PerfConfig(
                target,
                p.getInt("perf.warmup", 20),
                runs,
                p.getInt("perf.coldRuns", 5),
                p.getInt("perf.rejectionRuns", Math.max(10, runs / 10)),
                target == Target.UAT ? List.of(-1L) : parseSizes(p.get("perf.sizes", "1k,100k,1m,10m")),
                Path.of(p.get("perf.outputDir", "build/perf-results")),
                p.get("perf.protocol", "dataspace-protocol-http"),
                Boolean.parseBoolean(p.get("perf.retirement", "true")) && target == Target.PLAYGROUND,
                playground,
                uat);
    }

    /**
     * The settings that matter to read a run's results, safe to log and to write to the report
     * (no API key, no header value).
     */
    public Map<String, Object> describe() {
        var description = new LinkedHashMap<String, Object>();
        description.put("target", target.name().toLowerCase(Locale.ROOT));
        description.put("warmup", warmup);
        description.put("runs", runs);
        description.put("coldRuns", coldRuns);
        description.put("rejectionRuns", rejectionRuns);
        description.put("sizes", sizes);
        description.put("protocol", protocol);
        description.put("retirementPhase", retirementPhase);
        description.put("providerPublicEnabled", providerPublicEnabled());
        description.put("target settings", target == Target.PLAYGROUND ? playground.toString() : uat.toString());
        return description;
    }

    @Override
    public String toString() {
        return "PerfConfig" + describe();
    }

    public boolean providerPublicEnabled() {
        return target == Target.PLAYGROUND && !"disabled".equals(playground.providerPublicMode());
    }

    /**
     * Parses a comma-separated list of sizes in bytes, with optional k/m suffixes (powers of 1024).
     */
    static List<Long> parseSizes(String value) {
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(PerfConfig::parseSize)
                .toList();
    }

    static long parseSize(String value) {
        var lower = value.toLowerCase(Locale.ROOT);
        long multiplier = 1;
        if (lower.endsWith("k")) {
            multiplier = 1024;
        } else if (lower.endsWith("m")) {
            multiplier = 1024 * 1024;
        }
        var digits = multiplier == 1 ? lower : lower.substring(0, lower.length() - 1);
        var size = Long.parseLong(digits) * multiplier;
        if (size < 0) {
            throw new IllegalArgumentException("Negative payload size: " + value);
        }
        return size;
    }

    /**
     * Parses {@code Name: value; Other: value} into an ordered header map.
     */
    static Map<String, String> parseHeaders(String value) {
        var headers = new LinkedHashMap<String, String>();
        for (var entry : value.split(";")) {
            if (entry.isBlank()) {
                continue;
            }
            var separator = entry.indexOf(':');
            if (separator <= 0) {
                throw new IllegalArgumentException("Header '%s' is not in the 'Name: value' format".formatted(entry.trim()));
            }
            headers.put(entry.substring(0, separator).trim(), entry.substring(separator + 1).trim());
        }
        return headers;
    }

    private static void requireNonBlank(Map<String, String> values) {
        var missing = values.entrySet().stream().filter(e -> e.getValue().isBlank()).map(Map.Entry::getKey).sorted().toList();
        if (!missing.isEmpty()) {
            throw new IllegalStateException("perf.target=uat requires these properties: " + missing);
        }
    }

    private record Props(Function<String, String> source) {

        String get(String key, String defaultValue) {
            var value = source.apply(key);
            return value == null ? defaultValue : value;
        }

        int getInt(String key, int defaultValue) {
            var value = source.apply(key);
            return value == null ? defaultValue : Integer.parseInt(value.trim());
        }
    }
}
