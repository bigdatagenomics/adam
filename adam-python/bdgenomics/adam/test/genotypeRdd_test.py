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

    def check_for_line_in_file(self, path, line):

        try:
            fp = open(path)
            foundLine = False

            for l in fp:
                if l.strip().rstrip() == line:
                    foundLine = True
                    break
            
            self.assertTrue(foundLine)
        finally:
            fp.close()

    
    def test_vcf_round_trip(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().saveAsVcf(tmpPath)

        savedGenotypes = ac.loadGenotypes(testFile)

        self.assertEqual(genotypes._jvmRdd.jrdd().count(),
                          savedGenotypes._jvmRdd.jrdd().count())

        
    def test_vcf_add_filter(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addFilterHeaderLine("BAD",
                                                          "Bad variant.").saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath, '##FILTER=<ID=BAD,Description="Bad variant.">')


    def test_vcf_add_format_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addFixedArrayFormatHeaderLine("FA4",
                                                                    4,
                                                                    "Fixed array of 4 elements.",
                                                                    int).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##FORMAT=<ID=FA4,Number=4,Type=Integer,Description="Fixed array of 4 elements.">')


    def test_vcf_add_format_scalar(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addScalarFormatHeaderLine("SC",
                                                                "Scalar.",
                                                                str).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##FORMAT=<ID=SC,Number=1,Type=String,Description="Scalar.">')


    def test_vcf_add_format_genotype_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addGenotypeArrayFormatHeaderLine("GA",
                                                                       "Array with # genotypes.",
                                                                       float).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##FORMAT=<ID=GA,Number=G,Type=Float,Description="Array with # genotypes.">')

    def test_vcf_add_format_alts_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addAlternateAlleleArrayFormatHeaderLine("AA",
                                                                              "Array with # alts.",
                                                                              chr).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##FORMAT=<ID=AA,Number=A,Type=Character,Description="Array with # alts.">')

            
    def test_vcf_add_format_all_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addAllAlleleArrayFormatHeaderLine("RA",
                                                                        "Array with # alleles.",
                                                                        float).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##FORMAT=<ID=RA,Number=R,Type=Float,Description="Array with # alleles.">')


    def test_vcf_add_info_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addFixedArrayInfoHeaderLine("FA4",
                                                                  4,
                                                                  "Fixed array of 4 elements.",
                                                                  int).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##INFO=<ID=FA4,Number=4,Type=Integer,Description="Fixed array of 4 elements.">')


    def test_vcf_add_info_scalar(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addScalarInfoHeaderLine("SC",
                                                              "Scalar.",
                                                              bool).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##INFO=<ID=SC,Number=0,Type=Flag,Description="Scalar.">')


    def test_vcf_add_info_alts_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addAlternateAlleleArrayInfoHeaderLine("AA",
                                                                            "Array with # alts.",
                                                                            chr).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##INFO=<ID=AA,Number=A,Type=Character,Description="Array with # alts.">')

            
    def test_vcf_add_info_all_array(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().addAllAlleleArrayInfoHeaderLine("RA",
                                                                      "Array with # alleles.",
                                                                      float).saveAsVcf(tmpPath)

        self.check_for_line_in_file(tmpPath,
                                    '##INFO=<ID=RA,Number=R,Type=Float,Description="Array with # alleles.">')

            
    def test_vcf_sort(self):
    
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().sort().saveAsVcf(tmpPath,
                                                       asSingleFile=True)

        self.checkFiles(tmpPath, self.resourceFile("sorted.vcf", module='adam-cli'))


    def test_vcf_sort_lex(self):
    
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.ss)
        
        genotypes = ac.loadGenotypes(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        genotypes.toVariantContexts().sortLexicographically().saveAsVcf(tmpPath,
                                                                        asSingleFile=True)

        self.checkFiles(tmpPath, self.resourceFile("sorted.lex.vcf", module='adam-cli'))


    def test_transform(self):
        testFile = self.resourceFile("random.vcf")
        ac = ADAMContext(self.ss)

        genotypes = ac.loadGenotypes(testFile)

        transformedGenotypes = genotypes.transform(lambda x: x.filter(x.contigName == '1'))

        self.assertEqual(transformedGenotypes.toDF().count(), 9)


        
    def test_to_variants(self):
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)

        genotypes = ac.loadGenotypes(testFile)

        variants = genotypes.toVariants()

        self.assertEqual(variants.toDF().count(), 18)

        variants = genotypes.toVariants(dedupe=True)

        self.assertEqual(variants.toDF().count(), 6)

