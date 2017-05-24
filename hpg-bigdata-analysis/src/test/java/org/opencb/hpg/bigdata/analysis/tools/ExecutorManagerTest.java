package org.opencb.hpg.bigdata.analysis.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.Test;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by pfurio on 24/05/17.
 */
public class ExecutorManagerTest {

    @Test
    public void execute() throws AnalysisToolException, IOException, InterruptedException {
        Path path = Paths.get("/tmp");
        new ExecutorManager().execute("sleep 10", path);

        ObjectReader reader = new ObjectMapper().reader(Status.class);
        Status status = reader.readValue(path.resolve("status.json").toFile());
        assertEquals(Status.DONE, status.getName());
    }

}