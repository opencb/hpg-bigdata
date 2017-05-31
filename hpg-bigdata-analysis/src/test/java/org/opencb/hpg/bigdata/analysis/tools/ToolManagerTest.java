package org.opencb.hpg.bigdata.analysis.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.hpg.bigdata.analysis.exceptions.AnalysisToolException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 23/05/17.
 */
public class ToolManagerTest {

    @Test
    public void testToolManager() throws URISyntaxException, AnalysisToolException, IOException {
        Path toolPath = Paths.get(getClass().getResource("/tools").toURI());
        Path tmp = Paths.get("/tmp");

        ToolManager toolManager = new ToolManager(toolPath);
        ObjectMap params = new ObjectMap()
                .append("input", "/home/pfurio/Documents/bio_datasets/bams/ebi.bam")
                .append("output", "/tmp/test.bam.bai");
        String commandLine = toolManager.createCommandLine("samtools", "index", params);
        System.out.println(commandLine);
        Executor.execute(commandLine, tmp);

        ObjectReader reader = new ObjectMapper().reader(Status.class);
        Status status = reader.readValue(tmp.resolve("status.json").toFile());
        assertEquals(Status.DONE, status.getName());

        assertTrue(tmp.resolve("test.bam.bai").toFile().exists());
    }

}