package org.opencb.hpg.bigdata.tools.tasks.alignment.mr;

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
        seqFileName1 = null;
        seqFileName2 = null;
        resultFileName = null;
        indexFolderName = null;
    }
}
