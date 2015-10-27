/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package org.opencb.hpg.bigdata.core.io;

import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.opencb.biodata.tools.variant.converter.Converter;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mh719
 *
 */
public class VariantContextBlockIterator implements Converter<CharSequence, VariantContext> {

    private final VCFCodec codec;
    private boolean decodeGenotypes = false;

    public VariantContextBlockIterator(VCFCodec codec) {
        this.codec = codec;
    }

    public List<VariantContext> convert(List<CharBuffer> block) {
        int size = block.size();
        List<VariantContext> varList = new ArrayList<>(size);
        for (CharBuffer buff : block) {
            VariantContext ctx = convert(buff);
            if (decodeGenotypes) {
                if (ctx.getGenotypes().isLazyWithData()) {
                    ((LazyGenotypesContext) ctx.getGenotypes()).decode();
                }
            }
            varList.add(ctx);
        }
        return varList;
    }

    @Override
    public VariantContext convert(CharSequence buff) {
        return this.codec.decode(buff.toString());
    }

    public VCFCodec getCodec() {
        return codec;
    }

    public boolean isDecodeGenotypes() {
        return decodeGenotypes;
    }

    public void setDecodeGenotypes(boolean decodeGenotypes) {
        this.decodeGenotypes = decodeGenotypes;
    }

}
