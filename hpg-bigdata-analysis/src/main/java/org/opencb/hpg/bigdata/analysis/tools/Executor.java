package org.opencb.hpg.bigdata.analysis.tools;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.RunnableProcess;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by pfurio on 24/05/17.
 */
public class Executor {

    private static Logger logger = LoggerFactory.getLogger(Executor.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static int threadInitNumber;
    private static volatile String status;

    protected static void execute(String commandLine, Path outdir) throws AnalysisToolException {
        if (!outdir.toFile().isDirectory()) {
            throw new AnalysisToolException("Output directory " + outdir + " is not an actual directory");
        }
        if (!outdir.toFile().canWrite()) {
            throw new AnalysisToolException("Cannot write on output directory " + outdir);
        }

        try {
            status = Status.RUNNING;
            Command com = new Command(commandLine);

            DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(outdir.resolve("stdout.txt").toFile()));
            com.setOutputOutputStream(dataOutputStream);

            dataOutputStream = new DataOutputStream(new FileOutputStream(outdir.resolve("stderr.txt").toFile()));
            com.setErrorOutputStream(dataOutputStream);

            ExecutorMonitor monitor = new ExecutorMonitor();
//            Thread thread = new Thread(statusProcess, "StatusThread-" + nextThreadNum());

            Thread hook = new Thread(() -> {
//                status = Status.ERROR;
                monitor.stop(new Status(Status.ERROR));
                logger.info("Running ShutdownHook. Tool execution has being aborted.");
                com.setStatus(RunnableProcess.Status.KILLED);
                com.setExitValue(-2);
                closeOutputStreams(com);
            });

            logger.info("==========================================");
            logger.info("Executing tool");
            logger.debug("Executing commandLine {}", commandLine);
            logger.info("==========================================");
            System.err.println();

            Runtime.getRuntime().addShutdownHook(hook);
            monitor.start(outdir);
            com.run();
            Runtime.getRuntime().removeShutdownHook(hook);
            monitor.stop(new Status(Status.DONE));
//            status = Status.DONE;

            System.err.println();
            logger.info("==========================================");
            logger.info("Finished tool execution");
            logger.info("==========================================");

            closeOutputStreams(com);
        } catch (FileNotFoundException e) {
            logger.error("Could not create the output/error files", e);
        }
    }

    private static void closeOutputStreams(Command com) {
        /** Close output streams **/
        if (com.getOutputOutputStream() != null) {
            try {
                com.getOutputOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setOutputOutputStream(null);
            com.setOutput(null);
        }
        if (com.getErrorOutputStream() != null) {
            try {
                com.getErrorOutputStream().close();
            } catch (IOException e) {
                logger.warn("Error closing OutputStream", e);
            }
            com.setErrorOutputStream(null);
            com.setError(null);
        }
    }

    private static synchronized int nextThreadNum() {
        return threadInitNumber++;
    }

}
