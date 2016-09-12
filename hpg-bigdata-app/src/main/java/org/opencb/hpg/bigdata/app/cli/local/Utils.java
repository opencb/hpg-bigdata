package org.opencb.hpg.bigdata.app.cli.local;

import org.opencb.hpg.bigdata.core.lib.ParentDataset;

import java.io.File;

/**
 * Created by jtarraga on 12/09/16.
 */
public class Utils {
    public static void saveDatasetAsOneAvroFile(ParentDataset ds, String filename) {
        String tmpDir = filename + ".tmp";
        ds.coalesce(1).write().format("com.databricks.spark.avro").save(tmpDir);

        File dir = new File(tmpDir);
        if (!dir.isDirectory()) {
            // error management
            System.err.println("Error: a directory was expected but " + tmpDir);
            return;
        }

        // list out all the file name and filter by the extension
        Boolean found = false;
        String[] list = dir.list();
        for (String name: list) {
            if (name.startsWith("part-r-") && name.endsWith("avro")) {
                new File(tmpDir + "/" + name).renameTo(new File(filename));
                found = true;
                break;
            }
        }
        if (!found) {
            // error management
            System.err.println("Error: pattern 'part-r-*avro' was not found");
            return;
        }
        dir.delete();
    }
}
