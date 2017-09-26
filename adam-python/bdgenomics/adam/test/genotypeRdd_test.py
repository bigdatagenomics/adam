#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from bdgenomics.adam.adamContext import ADAMContext
from bdgenomics.adam.test import SparkTestCase


class GenotypeRDDTest(SparkTestCase):

    
    def test_vcf_round_trip(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.sc)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().saveAsVcf(tmpPath)

        savedGenotypes = ac.loadGenotypes(testFile)

        self.assertEquals(genotypes._jvmRdd.jrdd().count(),
                          savedGenotypes._jvmRdd.jrdd().count())

    
    def test_vcf_sort(self):
    
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.sc)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().sort().saveAsVcf(tmpPath,
                                                       asSingleFile=True)

        self.checkFiles(tmpPath, self.resourceFile("sorted.vcf"))


    def test_vcf_sort_lex(self):
    
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.sc)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().sortLexicographically().saveAsVcf(tmpPath,
                                                                        asSingleFile=True)

        self.checkFiles(tmpPath, self.resourceFile("sorted.lex.vcf"))


    def test_transform(self):
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.sc)

        genotypes = ac.loadGenotypes(testFile)

        transformedGenotypes = genotypes.transform(lambda x: x.filter(x.contigName == '1'))

        self.assertEquals(transformedGenotypes.toDF().count(), 9)
