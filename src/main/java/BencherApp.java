import com.google.common.base.Stopwatch;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.client.impl.script.VariableDefinition;

import io.deephaven.datagen.DataGen;

import io.deephaven.grpc_api.DeephavenChannel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;


public class BencherApp {

    public static String toPrettyString(Changes changes) {
        final StringBuilder sb = new StringBuilder();
        if (changes.errorMessage().isPresent()) {
            sb.append("Error: ").append(changes.errorMessage().get()).append(System.lineSeparator());
        }
        if (changes.isEmpty()) {
            sb.append("No displayable variables updated").append(System.lineSeparator());
        } else {
            for (VariableDefinition variableDefinition : changes.created()) {
                sb.append(variableDefinition.type()).append(' ').append(variableDefinition.title()).append(" = <new>")
                        .append(System.lineSeparator());
            }
            for (VariableDefinition variableDefinition : changes.updated()) {
                sb.append(variableDefinition.type()).append(' ').append(variableDefinition.title())
                        .append(" = <updated>")
                        .append(System.lineSeparator());
            }
            for (VariableDefinition variableDefinition : changes.removed()) {
                sb.append(variableDefinition.type()).append(' ').append(variableDefinition.title()).append(" <removed>")
                        .append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    static Session getSession() {

        String target = "localhost:10000";

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
        channelBuilder.usePlaintext();
        // channelBuilder.useTransportSecurity();
        channelBuilder.userAgent("DHMark");
        ManagedChannel managedChannel = channelBuilder.build();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        //TODO: set execution timeout
        SessionImplConfig cfg = SessionImplConfig.builder()
                .executor(scheduler)
                .channel(new DeephavenChannel(managedChannel))
                .build();

        return cfg.createSession();
    }

    static ConsoleSession getConsole(Session session) throws ExecutionException, InterruptedException {

        return session.console("python").get();
    }

    private static void onShutdown(ScheduledExecutorService scheduler,
                                   ManagedChannel managedChannel) {
        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Scheduler not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (!managedChannel.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Channel not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void runBenchmark(String inputFileName) throws IOException, ParseException {

        // get the JSON file root object as a map of column names to JSON objects
        JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(inputFileName));
        Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        ArrayList<Object> statements = (ArrayList<Object>) documentDictionary.get("statements");

        // get our console and a session within it
        try (final Session session = getSession()) {
            try (final ConsoleSession console = getConsole(session)) {

                // for each statement ...
                for (Object statementObject : statements) {

                    Map<String, Object> statementDefinitionDictionary = (Map<String, Object>) statementObject;

                    String title = (String) statementDefinitionDictionary.get("title");
                    String text = (String) statementDefinitionDictionary.get("text");
                    boolean isTimed = ((Long) statementDefinitionDictionary.get("timed")) != 0;

                    Stopwatch sw = isTimed ? Stopwatch.createStarted() : null;

                    // actually execute
                    try {
                        Changes changes = console.executeCode(text);
                    } catch (TimeoutException ex) {
                        System.err.printf("Execution of \"%s\" timed out: %s\n", title, ex.getMessage());
                        break;
                    }

                    // optionally time ...
                    if (sw != null) {
                        sw.stop();
                        System.out.printf("\"%s\": Execution took %d milliseconds\n", title, sw.elapsed(TimeUnit.MILLISECONDS));
                    }
                }
            } catch (ExecutionException e) {
                System.err.printf("Error: execution exception while getting a console: %s\n", e.getMessage());
                throw new InternalError(e.getMessage());
            } catch (InterruptedException e) {
                System.err.printf("Error: execution interrupted while getting a console: %s\n", e.getMessage());
                throw new InternalError(e.getMessage());
            }
        }
    }

    private static ArrayList<Object> getBenchmarks(String inputFileName) throws FileNotFoundException, IOException, ParseException {

        JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(inputFileName));
        Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        ArrayList<Object> benchmarks = (ArrayList<Object>) documentDictionary.get("benchmarks");

        return benchmarks;
    }

    public static void main(String[] args) {

        String inputFileName;
        if (args.length < 1) {
            inputFileName = "joiner-bench.json";
        } else {
            inputFileName = args[0];
        }

        // open and read the definition file to an array of definition objects
        ArrayList<Object> benchmarks = null;
        try {
            benchmarks = getBenchmarks(inputFileName);
        } catch (FileNotFoundException ex) {
            System.err.printf("Couldn't find file \"%s\": %s\n", inputFileName, ex.getMessage());
            System.exit(1);
        } catch (IOException ex) {
            System.err.printf("Couldn't read file \"%s\": %s\n", inputFileName, ex.getMessage());
            System.exit(1);
        } catch (ParseException ex) {
            System.err.printf("Couldn't parse file \"%s\": %s\n", inputFileName, ex.getMessage());
            System.exit(1);
        }

        // for each of the definition objects, run the benchmark!
        for (Object bench : benchmarks) {

            Map<String, Object> benchmarkDefinition = (Map<String, Object>) bench;

            String title = (String) benchmarkDefinition.get("title");
            String benchFile = (String) benchmarkDefinition.get("benchmark_file");
            ArrayList<String> generatorFiles = (ArrayList<String>) benchmarkDefinition.get("generator_files");

            System.out.printf("starting benchmark: \"%s\"\n", title);

            // generate data, then run the benchmark script
            for(String generatorFile : generatorFiles) {
                try {
                    DataGen.generateData(generatorFile);
                } catch (IOException ex) {
                    System.err.printf("Couldn't read generator file \"%s\": %s\n", generatorFile, ex.getMessage());
                    System.exit(1);
                } catch (ParseException ex) {
                    System.err.printf("Couldn't parse generator file \"%s\": %s\n", inputFileName, ex.getMessage());
                    System.exit(1);
                }
            }

            try {
                runBenchmark(benchFile);
            } catch (IOException ex) {
                System.err.printf("Couldn't read benchmark file \"%s\": %s\n", benchFile, ex.getMessage());
                System.exit(1);
            } catch (ParseException ex) {
                System.err.printf("Couldn't parse benchmark file \"%s\": %s\n", inputFileName, ex.getMessage());
                System.exit(1);
            }

            System.out.printf("benchmark \"%s\" completed\n", title);
        }

        System.exit(0);
    }
}
