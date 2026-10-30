package org.eclipse.edc.test.system.perf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * {@code kubectl port-forward} to an in-cluster service, on a random local port. Like the Kafka
 * tests (KafkaIntermediary), this shells out to the local kubectl with an explicit context rather
 * than going through SSH, so the scoped e2e-runner kubeconfig needs the {@code pods/portforward}
 * verb (see system-tests/readme.md).
 */
public final class PortForward implements AutoCloseable {

    private static final Pattern FORWARDING_LINE = Pattern.compile("Forwarding from 127\\.0\\.0\\.1:(\\d+) ->");

    private final Process process;
    private final int localPort;
    private final String description;

    private PortForward(Process process, int localPort, String description) {
        this.process = process;
        this.localPort = localPort;
        this.description = description;
    }

    /**
     * Starts the port-forward and blocks until kubectl reports the local port it bound.
     *
     * @param context    kubectl context, ignored when blank
     * @param namespace  namespace of the target resource
     * @param resource   e.g. {@code svc/provider-backend}
     * @param remotePort port on the target resource
     */
    public static PortForward open(String context, String namespace, String resource, int remotePort, Duration timeout) {
        var command = new ArrayList<>(List.of("kubectl"));
        if (context != null && !context.isBlank()) {
            command.addAll(List.of("--context", context));
        }
        command.addAll(List.of("-n", namespace, "port-forward", "--address", "127.0.0.1", resource, ":" + remotePort));
        var description = String.join(" ", command);

        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start '%s'".formatted(description), e);
        }

        var port = new CompletableFuture<Integer>();
        var output = new StringBuilder();
        var reader = new Thread(() -> readOutput(process, port, output), "port-forward-" + resource);
        reader.setDaemon(true);
        reader.start();

        try {
            return new PortForward(process, port.get(timeout.toMillis(), TimeUnit.MILLISECONDS), description);
        } catch (TimeoutException | ExecutionException e) {
            process.destroyForcibly();
            throw new IllegalStateException("'%s' did not report a local port within %s. Output:%n%s"
                    .formatted(description, timeout, output), e);
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for '%s'".formatted(description), e);
        }
    }

    /**
     * Keeps draining kubectl's output for the whole life of the process (otherwise its pipe fills
     * up and kubectl blocks), completing {@code port} on the first "Forwarding from" line.
     */
    private static void readOutput(Process process, CompletableFuture<Integer> port, StringBuilder output) {
        try (var lines = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                synchronized (output) {
                    output.append(line).append(System.lineSeparator());
                }
                var matcher = FORWARDING_LINE.matcher(line);
                if (!port.isDone() && matcher.find()) {
                    port.complete(Integer.parseInt(matcher.group(1)));
                }
            }
        } catch (IOException e) {
            port.completeExceptionally(e);
        }
        port.completeExceptionally(new IllegalStateException("kubectl exited with code " + process.exitValue()));
    }

    public int localPort() {
        return localPort;
    }

    public URI baseUri() {
        return URI.create("http://127.0.0.1:" + localPort);
    }

    public boolean isAlive() {
        return process.isAlive();
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return description + " (local port " + localPort + ")";
    }
}
