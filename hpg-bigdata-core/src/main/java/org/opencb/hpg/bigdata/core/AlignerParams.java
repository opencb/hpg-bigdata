package org.opencb.hpg.bigdata.core;

/**
 * Created by jtarraga on 29/05/15.
 */
public class AlignerParams {
    public int numSeeds;
    public float minSWScore;

    public String seqFileName1;
    public String seqFileName2;
    public String resultFileName;
    public String indexFolderName;

    public AlignerParams() {
        numSeeds = 20;
        minSWScore = 0.8f;

        seqFileName1 = new String("no-file-specified!");
        seqFileName2 = new String("no-file-specified!");
        resultFileName = new String("no-file-specified!");
        indexFolderName = new String("no-file-specified!");
    }
}
