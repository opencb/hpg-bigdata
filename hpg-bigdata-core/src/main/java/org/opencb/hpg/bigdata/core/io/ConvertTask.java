package org.opencb.hpg.bigdata.core.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.avro.VariantAvroAnnotator;
import org.opencb.hpg.bigdata.core.io.avro.AvroEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 04/10/17.
 */
public class ConvertTask implements ParallelTaskRunner.Task<VariantContext, VariantAvro> {
    private VariantContextToVariantConverter converter;
    private List<List<Predicate<VariantAvro>>> filters;
    private boolean annotate = false;

    protected final AvroEncoder<VariantAvro> encoder;

    public ConvertTask(VariantContextToVariantConverter converter, List<List<Predicate<VariantAvro>>> filters,
                       boolean annotate) {
        this.converter = converter;
        this.filters = filters;
        this.annotate = annotate;

        this.encoder = new AvroEncoder<>(VariantAvro.getClassSchema());
    }

    @Override
    public List<VariantAvro> apply(List<VariantContext> variantContexts) throws RuntimeException {
        List<VariantAvro> variantAvros;
        if (annotate) {
            // Annotate before converting to Avro
            // Duplicate code for efficiency purposes
            VariantAvroAnnotator variantAvroAnnotator = new VariantAvroAnnotator();
            List<VariantAvro> variants = new ArrayList<>(variantContexts.size());

            for (VariantContext vc : variantContexts) {
                try {
                    Variant variant = converter.convert(vc);
                    if (filter(variant.getImpl())) {
                        variants.add(variant.getImpl());
//                    statsCalculator.updateGlobalStats(variant);
                    }
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
            // Annotate variants and then write them to disk
            variantAvros = variantAvroAnnotator.annotate(variants);
        } else {
            // Convert to Avro without annotating
            variantAvros = new ArrayList<>(variantContexts.size());
            for (VariantContext vc : variantContexts) {
                Variant variant = converter.convert(vc);
                if (filter(variant.getImpl())) {
                    variantAvros.add(variant.getImpl());
                }
            }
        }

        // Return variants
        return variantAvros;
    }

    public boolean filter(VariantAvro record) {
        for (List<Predicate<VariantAvro>> list: filters) {
            if (list.size() == 1) {
                if (!list.get(0).test(record)) {
                    return false;
                }
            } else if (list.size() > 1) {
                boolean or = false;
                for (Predicate<VariantAvro> filter: list) {
                    if (filter.test(record)) {
                        or = true;
                        break;
                    }
                }
                if (!or) {
                    return false;
                }
            }
        }
        return true;
    }
}
