

import com.google.common.base.Stopwatch;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.client.impl.script.VariableDefinition;

import io.deephaven.datagen.DataGen;

import io.deephaven.datagen.Utils;
import io.deephaven.grpc_api.DeephavenChannel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

        String target = System.getProperty("dh.endpoint", "localhost:10000");

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

    static JSONObject getBenchmarkObject(final File dir, String inputFilename) throws IOException, ParseException {
        if (!inputFilename.startsWith(File.separator)) {
            inputFilename = dir.getAbsolutePath() + File.separator + inputFilename;
        }

        // get the JSON file root object as a map of column names to JSON objects
        final JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(inputFilename));
        return jsonMap;
    }

    static void runBenchmark(final File baseDir, final JSONObject jsonMap) {
        final Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        final ArrayList<Object> statements = (ArrayList<Object>) documentDictionary.get("statements");

        // get our console and a session within it
        try (final Session session = getSession()) {
            try (final ConsoleSession console = getConsole(session)) {

                // for each statement ...
                int statementNo = 0;
                for (final Object statementObject : statements) {
                    ++statementNo;
                    final Map<String, Object> statementDefinitionDictionary = (Map<String, Object>) statementObject;

                    final String title = (String) statementDefinitionDictionary.get("title");
                    if (title == null) {
                        throw new IllegalArgumentException(
                                "statement number " + statementNo + " should include a \"title\" element.");
                    }
                    final Object text = statementDefinitionDictionary.get("text");
                    final String statement;
                    if (text == null) {
                        final String statementFilename = (String) statementDefinitionDictionary.get("file");
                        if (statementFilename == null) {
                            throw new IllegalArgumentException(
                                    "statement number " + statementNo + " should include either \"text\" or \"file\" element.");
                        }
                        final File statementFile = Utils.locateFile(baseDir, statementFilename);
                        try {
                            statement = Files.lines(statementFile.toPath()).collect(Collectors.joining("\n")) + "\n";
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Error while trying to read statement file \"" +
                                            statementFile.getAbsolutePath() +
                                            "\" for statement number " + statementNo, e);
                        }
                    } else {
                        if (text instanceof String) {
                            statement = (String) text;
                        } else if (text instanceof ArrayList){
                            final ArrayList<Object> lines = (ArrayList<Object>) text;
                            final StringBuilder sb = new StringBuilder();
                            for (Object line : lines) {
                                sb.append(line.toString()).append('\n');
                            }
                            statement = sb.toString();
                        } else {
                            throw new IllegalArgumentException(
                                    "element \"text\" has the wrong type, it should be either string or array of strings");
                        }
                    }
                    boolean isTimed = ((Long) statementDefinitionDictionary.get("timed")) != 0;

                    Stopwatch sw = isTimed ? Stopwatch.createStarted() : null;

                    // actually execute
                    Changes changes = null;
                    try {
                        changes = console.executeCode(statement);
                    } catch (TimeoutException ex) {
                        System.err.printf("Execution of \"%s\" timed out: %s\n", title, ex.getMessage());
                        System.exit(1);
                    }

                    final Optional<String> errorMessageOptional = changes.errorMessage();
                    errorMessageOptional.ifPresent(s -> {
                        System.err.printf("Execution of \"%s\" failed with error:\n%s\n", title, s);
                        System.exit(1);
                    });

                    // optionally time ...
                    if (sw != null) {
                        sw.stop();
                        System.out.printf("\"%s\": Execution took %d milliseconds\n", title, sw.elapsed(TimeUnit.MILLISECONDS));
                        System.out.flush();
                    }
                }
            } catch (ExecutionException e) {
                System.err.printf("Error: execution exception while getting a console: %s\n", e.getMessage());
                throw new RuntimeException(e.getMessage());
            } catch (InterruptedException e) {
                System.err.printf("Error: execution interrupted while getting a console: %s\n", e.getMessage());
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private static ArrayList<Object> getBenchmarks(String jobFilename) throws IOException, ParseException {

        JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(jobFilename));
        Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        ArrayList<Object> benchmarks = (ArrayList<Object>) documentDictionary.get("benchmarks");

        return benchmarks;
    }

    private static final String JOBS_PREFIX_PATH = System.getProperty("jobs.prefix.path", "jobs");

    private static String relJsonFn(final String fn) {
        if (!fn.startsWith(File.separator) && JOBS_PREFIX_PATH != null) {
            return JOBS_PREFIX_PATH + File.separator + fn;
        }
        return fn;
    }

    private static final String me = BencherApp.class.getSimpleName();

    public static void main(String[] args) {

        String jobFilename = null;
        if (args.length != 1) {
            System.err.println("Usage: " + me + " job.json");
            System.exit(1);
        } else {
            jobFilename = relJsonFn(args[0]);
        }

        final File jobFile = new File(jobFilename);
        if (!jobFile.exists()) {
            System.err.printf(me + ": job file \"%s\" doesn't exist.", jobFilename);
        }
        final File inputFileDir = jobFile.getParentFile();

        // open and read the definition file to an array of definition objects
        ArrayList<Object> benchmarks = null;
        try {
            benchmarks = getBenchmarks(jobFilename);
        } catch (FileNotFoundException ex) {
            System.err.printf(me + ": Couldn't find file \"%s\": %s\n", jobFilename, ex.getMessage());
            System.exit(1);
        } catch (IOException ex) {
            System.err.printf(me + ": Couldn't read file \"%s\": %s\n", jobFilename, ex.getMessage());
            System.exit(1);
        } catch (ParseException ex) {
            System.err.printf(me + ": Couldn't parse file \"%s\": %s\n", jobFilename, ex.getMessage());
            System.exit(1);
        }

        // for each of the definition objects, run the benchmark!
        int benchmarkNo = 0;
        for (Object bench : benchmarks) {
            ++benchmarkNo;
            Map<String, Object> benchmarkDefinition = (Map<String, Object>) bench;

            final String title = (String) benchmarkDefinition.get("title");
            if (title == null) {
                System.err.println(me + ": benchmark number " + benchmarkNo + " is missing a \"title\" element");
            }
            ArrayList<String> generatorFilenames = (ArrayList<String>) benchmarkDefinition.get("generator_files");
            for (int i = 0; i < generatorFilenames.size(); ++i) {
                generatorFilenames.set(i, generatorFilenames.get(i));
            }

            System.out.printf("Starting for benchmark name \"%s\" from file \"%s\"\n", title, jobFile.getAbsoluteFile());

            // generate data, then run the benchmark script
            for (String generatorFilename : generatorFilenames) {
                try {
                    DataGen.generateData(inputFileDir, generatorFilename);
                } catch (IOException ex) {
                    System.err.printf(me + ": Couldn't read generator file \"%s\": %s\n", generatorFilename, ex.getMessage());
                    System.exit(1);
                } catch (ParseException ex) {
                    System.err.printf(me  + ": Couldn't parse generator file \"%s\": %s\n", jobFilename, ex.getMessage());
                    System.exit(1);
                }
            }

            if (Boolean.parseBoolean(System.getProperty("generate.only", "False"))) {
                System.out.printf("Generate only requested, not running benchmark \"%s\"", title);
                continue;
            }
            final JSONObject benchmarkObject;
            final String benchFilename = (String) benchmarkDefinition.get("benchmark_file");
            try {
                if (benchFilename != null) {
                    benchmarkObject = getBenchmarkObject(inputFileDir, benchFilename);
                } else {
                    benchmarkObject = (JSONObject) benchmarkDefinition.get("benchmark");
                    if (benchmarkObject == null) {
                        System.err.printf(me + ": There is no \"benchmark_file\" or \"benchmark\" definition in \"%s\"\n",
                                jobFile.getAbsolutePath());
                        System.exit(1);
                    }
                }
                runBenchmark(inputFileDir, benchmarkObject);
            } catch (IOException ex) {
                if (benchFilename != null) {
                    System.err.printf(me + ": Couldn't read benchmark file \"%s\": %s\n", benchFilename, ex.getMessage());
                } else {
                    System.err.printf(me + ": Couldnt read benchmark: %s", ex.getMessage());
                }
                System.exit(1);
            } catch (ParseException ex) {
                System.err.printf(me + ": Couldn't parse benchmark file \"%s\": %s\n",
                        jobFile.getAbsolutePath(), ex.getMessage());
                System.exit(1);
            } catch (Exception ex) {
                System.err.printf(me + ": Execution failed for \"%s\": %s\n",
                        title, ex.getMessage());
                System.exit(1);
            }

            System.out.printf("benchmark \"%s\" completed\n", title);
        }

        System.exit(0);
    }
}
