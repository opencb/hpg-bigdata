package org.opencb.hpg.bigdata.core.converters.variation;

import com.google.protobuf.GeneratedMessage;
import org.opencb.biodata.tools.Converter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 12/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProtoEncoderTask<T extends GeneratedMessage> implements ParallelTaskRunner.Task<CharSequence, ByteBuffer> {

    private final Converter<CharSequence, T> converter;
    private final int bufferSize;
    private int maxBufferSize;

    private static final int LOG_BATCH_SIZE = 1000;
    private static AtomicLong numConverts = new AtomicLong(0);
    private static Logger logger = LoggerFactory.getLogger(ProtoEncoderTask.class.toString());

    public ProtoEncoderTask(Converter<CharSequence, T> converter, int bufferSize) {
        this.converter = converter;
        this.bufferSize = bufferSize;
        this.maxBufferSize = bufferSize;
    }

    public static class ByteBufferOutputStream extends ByteArrayOutputStream {
        public ByteBufferOutputStream() {
        }

        public ByteBufferOutputStream(int size) {
            super(size);
        }

        public ByteBuffer toByteBuffer() {
            return ByteBuffer.wrap(buf, 0, count);
        }
    }

    @Override
    public void pre() {
        maxBufferSize = bufferSize;
    }

    @Override
    public List<ByteBuffer> apply(List<CharSequence> batch) {


        List<T> converted = converter.apply(batch);

        ByteBufferOutputStream outputStream = new ByteBufferOutputStream(maxBufferSize);
        try {
            for (T element : converted) {
                element.writeDelimitedTo(outputStream);
            }
            logProgress(batch.size());
            ByteBuffer byteBuffer = outputStream.toByteBuffer();
            maxBufferSize = Math.max(maxBufferSize, byteBuffer.array().length);
            return Collections.singletonList(byteBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void logProgress(int size) {
        long num = numConverts.getAndAdd(size);
        long batch = num / LOG_BATCH_SIZE;
        long newBatch = (num + size) / LOG_BATCH_SIZE;
        logger.debug("Another batch of: " + size);
        if (batch != newBatch) {
            logger.info("Num processed variants: " + newBatch * LOG_BATCH_SIZE);
        }
    }


}
