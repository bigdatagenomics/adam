import org.bdgenomics.adam.avro.Read
import org.bdgenomics.adam.avro.NucleotideContigFragment
import org.bdgenomics.adam.avro.Pileup
import org.bdgenomics.adam.avro.ADAMNestedPileup
import org.bdgenomics.adam.avro.Contig
import org.bdgenomics.adam.avro.Variant
import org.bdgenomics.adam.avro.VariantCallingAnnotations
import org.bdgenomics.adam.avro.Genotype
import org.bdgenomics.adam.avro.VariantEffect
import org.bdgenomics.adam.avro.DatabaseVariantAnnotation
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
println("\nYou can call ADAMContext methods on sc")
println("Load a file using: sc.adamLoad(\"<Path to file>\");")
