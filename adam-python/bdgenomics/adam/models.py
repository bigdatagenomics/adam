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
r"""
======
models
======
.. currentmodule:: bdgenomics.adam.models
.. autosummary::
   :toctree: _generate/

   ReferenceRegion
"""

class ReferenceRegion:
    """
    Represents a contiguous region of the reference genome.
    """

    def __init__(self, referenceName, start, end):
        """
        Represents a contiguous region of the reference genome.

        :param referenceName The name of the sequence (chromosome) in the reference genome
        :param start The 0-based residue-coordinate for the start of the region
        :param end The 0-based residue-coordinate for the first residue <i>after</i> the start
        which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
        half-open interval.
        """

        self.referenceName = referenceName
        self.start = start
        self.end = end


    def _toJava(self, jvm):
        """
        Converts to an org.bdgenomics.adam.models.ReferenceRegion

        Should not be called from user code.

        :param jvm: Py4j JVM handle.
        """

        return jvm.org.bdgenomics.adam.models.ReferenceRegion.fromGenomicRange(self.referenceName, self.start, self.end)
