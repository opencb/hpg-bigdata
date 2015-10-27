
package org.opencb.hpg.bigdata.tools.variant;
       import java.io.IOException;

        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.InputSplit;
        import org.apache.hadoop.mapreduce.RecordReader;
        import org.apache.hadoop.mapreduce.TaskAttemptContext;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public  class NLinesInputFormat extends FileInputFormat<LongWritable, Text> {
    private static final long MAX_SPLIT_SIZE = 100000;  //1KB SPLIT

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputsplit,
                                                              TaskAttemptContext taskattemptcontext) throws IOException,
            InterruptedException {

        return new NLinesRecordReader();
    }
    @Override
    protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
        return super.computeSplitSize(blockSize, minSize, MAX_SPLIT_SIZE);
    }

}
