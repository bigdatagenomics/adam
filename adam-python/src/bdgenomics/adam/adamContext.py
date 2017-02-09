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

from bdgenomics.adam.rdd import AlignmentRecordRDD, \
    FeatureRDD, \
    FragmentRDD, \
    GenotypeRDD, \
    NucleotideContigFragmentRDD, \
    VariantRDD
    

class ADAMContext(object):
    """
    The ADAMContext provides functions on top of a SparkContext for loading
    genomic data.
    """


    def __init__(self, sc):
        """
        Initializes an ADAMContext using a SparkContext.

        :param pyspark.context.SparkContext sc: The currently active
        SparkContext.
        """

        self._sc = sc
        self._jvm = sc._jvm
        c = self._jvm.org.bdgenomics.adam.rdd.ADAMContext(sc._jsc.sc())
        self.__jac = self._jvm.org.bdgenomics.adam.api.java.JavaADAMContext(c)


    def loadAlignments(self, filePath):
        """
        Loads in an ADAM read file. This method can load SAM, BAM, and ADAM files.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing reads.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        adamRdd = self.__jac.loadAlignments(filePath)

        return AlignmentRecordRDD(adamRdd, self._sc)


    def loadSequence(self, filePath):
        """
        Loads in sequence fragments.

        Can load from FASTA or from Parquet encoded NucleotideContigFragments.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing sequence fragments.
        :rtype: bdgenomics.adam.rdd.NucleotideContigFragmentRDD
        """

        adamRdd = self.__jac.loadSequences(filePath)

        return NucleotideContigFragmentRDD(adamRdd, self._sc)


    def loadFragments(self, filePath):
        """
        Loads in read pairs as fragments.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing sequenced fragments.
        :rtype: bdgenomics.adam.rdd.FragmentRDD
        """

        adamRdd = self.__jac.loadFragments(filePath)

        return FragmentRDD(adamRdd, self._sc)


    def loadFeatures(self, filePath):
        """
        Loads in genomic features.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing features.
        :rtype: bdgenomics.adam.rdd.FeatureRDD
        """

        adamRdd = self.__jac.loadFeatures(filePath)

        return FeatureRDD(adamRdd, self._sc)


    def loadGenotypes(self, filePath):
        """
        Loads in genotypes.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing genotypes.
        :rtype: bdgenomics.adam.rdd.GenotypeRDD
        """

        adamRdd = self.__jac.loadGenotypes(filePath)

        return GenotypeRDD(adamRdd, self._sc)


    def loadVariants(self, filePath):
        """
        Loads in variants.

        :param str filePath: The path to load the file from.
        :return: Returns an RDD containing variants.
        :rtype: bdgenomics.adam.rdd.VariantRDD
        """

        adamRdd = self.__jac.loadVariants(filePath)

        return VariantRDD(adamRdd, self._sc)
