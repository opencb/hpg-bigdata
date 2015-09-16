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

package org.opencb.hpg.bigdata.app.cli.hadoop;

import org.ga4gh.models.Variant;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.converters.mr.Vcf2AvroMR;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;

/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
//		super(fastqCommandOptions.logLevel, fastqCommandOptions.verbose, fastqCommandOptions.conf);

        this.variantCommandOptions = variantCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        String subCommandString = variantCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "convert":
                init(variantCommandOptions.convertVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.conf);
                convert();
            case "index":
                init(variantCommandOptions.indexVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.conf);
                index();
                break;
        }
    }

    private void convert() throws Exception {
        String input = variantCommandOptions.convertVariantCommandOptions.input;
        String output = variantCommandOptions.convertVariantCommandOptions.output;
        String compression = variantCommandOptions.convertVariantCommandOptions.compression;

        if (output == null) {
            output = input;
        }

        // clean paths
//        String in = PathUtils.clean(input);
//        String out = PathUtils.clean(output);

        if (variantCommandOptions.convertVariantCommandOptions.toParquet) {
            logger.info("Transform {} to parquet", input);

            new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
//            if (PathUtils.isHdfs(input)) {
//                new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
//            } else {
//                new ParquetConverter<Variant>(Variant.getClassSchema()).toParquet(new FileInputStream(input), output);
//            }

        } else {
            Vcf2AvroMR.run(input, output, compression);
        }
    }

    private void index() throws Exception {
        System.out.println("Hello guys!!");
    }

}
