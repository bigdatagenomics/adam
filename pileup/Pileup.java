package edu.berkeley.cs.amplab.adam.modules.pileup;

import edu.berkeley.cs.amplab.adam.avro.ADAMPileup;
import edu.berkeley.cs.amplab.adam.avro.ReferencePosition;
import edu.berkeley.cs.amplab.adam.modules.ImportSupport;
import edu.berkeley.cs.amplab.adam.util.Predicates;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import parquet.avro.AvroParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

public class Pileup extends ImportSupport {

    public static String MIN_MAPQ_CONFIG = "adam.pileup.mapqMin";
    public static Long MIN_MAPQ_DEFAULT = 30L;

    @Override
    public String getModuleName() {
        return "pileup";
    }

    @Override
    public String getModuleDescription() {
        return "Create a pileup file from ADAM reference and read data";
    }

    @Option(name = "-reads", required = true, usage = "ADAM read file")
    String readInput;

    @Option(name = "-mapq", usage = "Minimal mapq value allowed for a read. Default = 30")
    Long minMapq = MIN_MAPQ_DEFAULT;

    @Argument(required = true, usage = "The new rod file to create")
    String pileupOutput;

    @Override
    public int moduleRun() throws Exception {
        Job job = setupJob();
        Configuration config = ContextUtil.getConfiguration(job);
        config.setLong(MIN_MAPQ_CONFIG, minMapq);

        AvroParquetInputFormat.addInputPath(job, new Path(readInput));
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setUnboundRecordFilter(job, Predicates.LocusWalker.class);

        AvroJob.setMapOutputKeySchema(job, ReferencePosition.SCHEMA$);
        AvroJob.setMapOutputValueSchema(job, ADAMPileup.SCHEMA$);

        job.setMapperClass(PileupReadMapper.class);
        job.setReducerClass(PileupReducer.class);

        setupOutputFormat(job, ADAMPileup.SCHEMA$, new Path(pileupOutput));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Pileup(), args));
    }
}
