package org.opencb.hpg.bigdata.analysis.tools;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by pfurio on 31/05/17.
 */
public class ExecutorMonitor {

    private static Logger logger = LoggerFactory.getLogger(Executor.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static int threadInitNumber;

    private Path directory;
    private final int TIME = 5000;
    private Thread myThread;

    public void start(Path directory) {
        if (!directory.toFile().isDirectory()) {
            logger.error("Output directory {} is not an actual directory", directory);
            return;
        }
        if (!directory.toFile().canWrite()) {
            logger.error("Cannot write on output directory {}", directory);
            return;
        }

        this.directory = directory;

        Runnable statusProcess = () -> {
            // Create hook to write status with ERROR
            Thread hook = new Thread(() -> {
                Status statusObject = new Status(Status.ERROR);
                try {
                    objectMapper.writer().writeValue(this.directory.resolve("status.json").toFile(), statusObject);
                } catch (IOException e) {
                    logger.error("Could not write status {} to file", Status.ERROR, e);
                }
            });
            Runtime.getRuntime().addShutdownHook(hook);

            // Keep writing in the status file while running to update current time
            Status statusObject = new Status(Status.RUNNING);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    statusObject.setCurrentDate();
                    objectMapper.writer().writeValue(this.directory.resolve("status.json").toFile(), statusObject);
                } catch (IOException e) {
                    logger.error("Could not write status {} to file", Status.RUNNING, e);
                }

                try {
                    Thread.sleep(this.TIME);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            Runtime.getRuntime().removeShutdownHook(hook);
        };

        myThread = new Thread(statusProcess, "StatusThread-" + nextThreadNum());
        myThread.start();
    }

    public void stop(Status status) {
        if (this.directory == null) {
            return;
        }

        // Interrupt and wait for the thread
        myThread.interrupt();
        try {
            myThread.join();
        } catch (InterruptedException e) {
            logger.error("Thread is alive? {}", myThread.isAlive() ? "true" : "false");
        }

        // Write the status
        try {
            objectMapper.writer().writeValue(this.directory.resolve("status.json").toFile(), status);
        } catch (IOException e) {
            logger.error("Could not write status {} to file", status.getName(), e);
        }
    }

    private static synchronized int nextThreadNum() {
        return threadInitNumber++;
    }

}
