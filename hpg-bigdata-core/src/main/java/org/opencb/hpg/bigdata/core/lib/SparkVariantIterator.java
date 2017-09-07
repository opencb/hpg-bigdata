package org.opencb.hpg.bigdata.core.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.Variant;

import java.io.IOException;
import java.util.Iterator;

public class SparkVariantIterator implements Iterator<Variant> {
    private Iterator<String> strIterator;
    private ObjectMapper objMapper;

    public SparkVariantIterator(VariantDataset variantDataset) {
        strIterator = variantDataset.toJSON().toLocalIterator();
        objMapper = new ObjectMapper();
    }

    @Override
    public boolean hasNext() {
        return strIterator.hasNext();
    }

    @Override
    public Variant next() {
        try {
            return objMapper.readValue(strIterator.next(), Variant.class);
        } catch (IOException e) {
            e.printStackTrace();
            return new Variant();
        }
    }
}
