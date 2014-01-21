/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.models.{ReferenceRegion, ADAMVariantContext}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.PhredUtils._
import java.io.File
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField

class AdamContextSuite extends SparkFunSuite {

  test("referenceLengthFromCigar") {
    import AdamContext._
    assert( referenceLengthFromCigar("3M") === 3 )
    assert( referenceLengthFromCigar("30M") === 30 )
    assert( referenceLengthFromCigar("10Y") === 0 ) // should abort when it hits an illegal operator
    assert( referenceLengthFromCigar("10M1Y" ) === 10 ) // same
    assert( referenceLengthFromCigar("10M1I10M" ) === 20 )
    assert( referenceLengthFromCigar("10M1D10M" ) === 21 )
    assert( referenceLengthFromCigar("1S10M1S") === 10 )
  }

  sparkTest("sc.adamLoad should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert(reads.count() === 20)
  }

  sparkTest("can read a small .VCF file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.vcf").getFile
    val variants: RDD[ADAMVariantContext] = sc.adamVariantContextLoad(path)
    assert(variants.count() === 4)
  }

  test("Can convert to phred") {
    assert(doubleToPhred(0.9) === 10)
    assert(doubleToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToDouble(10) > 0.89 && phredToDouble(10) < 0.91)
    assert(phredToDouble(50) > 0.99998 && phredToDouble(50) < 0.999999)
  }

  sparkTest("adamRecordsRegionParquetLoad can retrieve a simple subset of reads") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val dict = sc.adamDictionaryLoad[ADAMRecord](path)
    val pathReads : RDD[ADAMRecord] = sc.adamLoad(path)

    val tempFile = File.createTempFile("adam", "small.sam")
    val tmpAdamPath = new File(tempFile.getParent, tempFile.getName + ".1").getAbsolutePath
    pathReads.adamSave(tmpAdamPath)

    val region = ReferenceRegion(dict("1").id, 37577446, 57577446)

    val filteredLoadedReads = sc.adamRecordsRegionParquetLoad(tmpAdamPath, region, Set(ADAMRecordField.readName))

    assert( filteredLoadedReads.count() === 2 )
  }

}

