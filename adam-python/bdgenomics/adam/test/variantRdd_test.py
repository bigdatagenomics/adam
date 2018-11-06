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


class VariantRDDTest(SparkTestCase):

    
    def test_vcf_round_trip(self):
        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        variants = ac.loadVariants(testFile)

        tmpPath = self.tmpFile() + ".vcf"
        variants.toVariantContexts().saveAsVcf(tmpPath)

        savedVariants = ac.loadVariants(testFile)

        self.assertEqual(variants._jvmRdd.jrdd().count(),
                          savedVariants._jvmRdd.jrdd().count())


    def test_transform(self):

        variantPath = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)

        variants = ac.loadVariants(variantPath)

        transformedVariants = variants.transform(lambda x: x.filter(x.start < 19190))

        self.assertEqual(transformedVariants.toDF().count(), 3)
