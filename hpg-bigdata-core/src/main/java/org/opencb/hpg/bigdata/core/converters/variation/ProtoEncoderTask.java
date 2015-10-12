package org.opencb.hpg.bigdata.core.converters.variation;

import com.google.protobuf.GeneratedMessage;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 12/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProtoEncoderTask<T extends GeneratedMessage> implements ParallelTaskRunner.Task<CharBuffer, ByteBuffer> {

    private final Converter<CharBuffer, T> converter;
    private final int bufferSize;

    private static final int LOG_BATCH_SIZE = 1000;
    private static AtomicLong numConverts = new AtomicLong(0);
    private static Logger logger = LoggerFactory.getLogger(ProtoEncoderTask.class.toString());

    public ProtoEncoderTask(Converter<CharBuffer, T> converter, int bufferSize) {
        this.converter = converter;
        this.bufferSize = bufferSize;
    }

    @Override
    public List<ByteBuffer> apply(List<CharBuffer> batch) {

        logProgress(batch.size());

        List<T> converted = converter.apply(batch);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bufferSize);
        try {
            for (T element : converted) {
                element.writeDelimitedTo(outputStream);
            }
            return Collections.singletonList(ByteBuffer.wrap(outputStream.toByteArray()));
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
