import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.avro.ADAMNucleotideContigFragment
import org.bdgenomics.adam.avro.ADAMPileup
import org.bdgenomics.adam.avro.ADAMNestedPileup
import org.bdgenomics.adam.avro.ADAMContig
import org.bdgenomics.adam.avro.ADAMVariant
import org.bdgenomics.adam.avro.VariantCallingAnnotations
import org.bdgenomics.adam.avro.ADAMGenotype
import org.bdgenomics.adam.avro.VariantEffect
import org.bdgenomics.adam.avro.ADAMDatabaseVariantAnnotation
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
println("\nYou can call ADAMContext methods on sc")
println("Load a file using: sc.adamLoad(\"<Path to file>\");")
