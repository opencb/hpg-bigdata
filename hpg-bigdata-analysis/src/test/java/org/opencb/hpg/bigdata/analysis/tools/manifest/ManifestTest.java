package org.opencb.hpg.bigdata.analysis.tools.manifest;


import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by pfurio on 23/05/17.
 */
public class ManifestTest {

    @Test
    public void testDefault() {
        Manifest manifest = new Manifest();

        manifest.setAuthor(new Author("John", "john@email.org"));
        manifest.setDescription("Manifest description");
        manifest.setGit("Git commit");
        manifest.setId("Tool id");
        manifest.setName("Tool name");
        manifest.setVersion("v1");
        manifest.setPublication("URL to publication");
        manifest.setLanguage(new ProgrammingLanguage("bash", "v1", Collections.emptyList()));
        manifest.setSeparator(" ");
        manifest.setWebsite("http://www.mywebsite.org");

        Execution execution1 = new Execution("exec1", "Execution 1", "tool exec1", "", true, Arrays.asList(
                new InputParam("input", "Input blabla", null, 1, true, false, false, 0, false, null, null, InputParam.Type.STRING),
                new InputParam("output", "Output blabla", null, 1, true, true, false, 0, false, null, null, InputParam.Type.STRING)
        ));

        Execution execution2 = new Execution("exec2", "Execution 2", "tool exec2", "", true, Arrays.asList(
                new InputParam("input", "Input blabla", null, 1, true, false, false, 0, false, null, null, InputParam.Type.STRING),
                new InputParam("output", "Output blabla", null, 1, true, true, false, 0, false, null, null, InputParam.Type.STRING)
        ));

        manifest.setExecutions(Arrays.asList(execution1, execution2));

        try {
            manifest.serialize(new FileOutputStream("/tmp/manifest-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {
        Manifest manifest = Manifest.load(getClass().getResource("/manifest-test.yml").openStream());
        System.out.println("Manifest = " + manifest);
    }

}