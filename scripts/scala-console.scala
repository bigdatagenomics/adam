import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.formats.avro.Contig
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.formats.avro.Pileup
import org.bdgenomics.formats.avro.Variant
import org.bdgenomics.formats.avro.VariantCallingAnnotations
import org.bdgenomics.formats.avro.VariantEffect
println("\nYou can call ADAMContext methods on sc")
println("Load a file using: sc.adamLoad(\"<Path to file>\");")
