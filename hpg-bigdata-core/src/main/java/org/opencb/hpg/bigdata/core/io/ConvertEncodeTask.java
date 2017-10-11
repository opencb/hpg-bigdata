package org.opencb.hpg.bigdata.core.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.avro.VariantAvroAnnotator;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 04/10/17.
 */
public class ConvertEncodeTask implements ParallelTaskRunner.Task<VariantContext, ByteBuffer> {
    private ConvertTask convertTask;
    protected final AvroEncoder<VariantAvro> encoder;

    public ConvertEncodeTask(VariantContextToVariantConverter converter, List<List<Predicate<VariantAvro>>> filters,
                             VariantAvroAnnotator annotator) {
        convertTask = new ConvertTask(converter, filters, annotator);
        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    @Override
    public List<ByteBuffer> apply(List<VariantContext> variantContexts) throws RuntimeException {
        List<VariantAvro> variantAvros = convertTask.apply(variantContexts);

        // Encode
        List<ByteBuffer> encoded;
        try {
            encoded = encoder.encode(variantAvros);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return encoded;
    }
}
