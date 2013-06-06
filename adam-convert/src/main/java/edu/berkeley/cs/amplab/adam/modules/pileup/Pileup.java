package edu.berkeley.cs.amplab.adam.modules.pileup;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup;
import edu.berkeley.cs.amplab.adam.avro.PileupFragment;
import edu.berkeley.cs.amplab.adam.avro.PileupFragmentId;
import edu.berkeley.cs.amplab.adam.modules.ImportSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

import java.util.List;
import java.util.Map;

public class Pileup extends ImportSupport {

    public static String MIN_MAPQ_CONFIG = "adam.pileup.mapqMin";
    public static Long MIN_MAPQ_DEFAULT = 30L;
    public static String PILEUP_STEP = "adam.pileup.step";
    public static Long PILEUP_STEP_DEFAULT = 1000L;
    public static String PILEUP_REFERENCE_MAP = "adam.pileup.referenceMap";

    @Override
    public String getModuleName() {
        return "pileup";
    }

    @Override
    public String getModuleDescription() {
        return "Create a pileup file from ADAM reference and read data";
    }

    @Option(name = "-reference", required = true, usage = "ADAM reference data")
    String referenceInput;

    @Option(name = "-reads", required = true, usage = "ADAM read data")
    String readInput;

    @Option(name = "-mapq", usage = "Minimal mapq value allowed for a read. Default = 30")
    Long minMapq = MIN_MAPQ_DEFAULT;

    @Option(name = "-step", usage = "Number of reference positions per bucket. Default = 1000")
    Long step = PILEUP_STEP_DEFAULT;

    @Argument(required = true, usage = "The new pileup file to create")
    String pileupOutput;

    @Option(name = "-rename_reference", usage = "Remap read reference name, e.g. -mapReference 11=chr11")
    List<String> renamedReferences = Lists.newArrayList();
    static String REFERENCE_DELIMITER = ";";
    static Joiner.MapJoiner REFERENCE_NAME_JOINER = Joiner.on(REFERENCE_DELIMITER).withKeyValueSeparator("=");
    static Splitter.MapSplitter REFERENCE_NAME_SPLITTER = Splitter.on(REFERENCE_DELIMITER).withKeyValueSeparator("=");

    /**
     * In order to use MultipleInputs with AvroParquetInput and GenericRecords, we need to
     * fool MultipleInputs into thinking that the input formats are different (since they
     * are and use difference schemas: ADAMRecord and ADAMFastaFragment in this case).
     */
    public static class MyInputWorkaround extends ParquetInputFormat<GenericRecord> {
        public MyInputWorkaround() {
            super(AvroReadSupport.class);
        }
    }

    Map<String, String> generateReferenceMap(List<String> options) {
        Map<String, String> map = Maps.newHashMap();
        Splitter equalSplitter = Splitter.on("=");
        for (String option : options) {
            List<String> keyValue = Lists.newArrayList(equalSplitter.split(option));
            if (keyValue.size() == 2) {
                map.put(keyValue.get(0), keyValue.get(1));
            }
        }
        return map;
    }

    @Override
    public int moduleRun() throws Exception {
        Job job = setupJob();
        Configuration config = ContextUtil.getConfiguration(job);
        config.setLong(MIN_MAPQ_CONFIG, minMapq);
        config.setLong(PILEUP_STEP, step);
        config.set(PILEUP_REFERENCE_MAP, REFERENCE_NAME_JOINER.join(generateReferenceMap(renamedReferences)));

        MultipleInputs.addInputPath(job, new Path(referenceInput), MyInputWorkaround.class, PileupReferenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(readInput), AvroParquetInputFormat.class, PileupReadMapper.class);
        AvroJob.setMapOutputKeySchema(job, PileupFragmentId.SCHEMA$);
        AvroJob.setMapOutputValueSchema(job, PileupFragment.SCHEMA$);

        job.setReducerClass(PileupReducer.class);

        setupOutputFormat(job, ADAMPileup.SCHEMA$, new Path(pileupOutput));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Pileup(), args));
    }
}
