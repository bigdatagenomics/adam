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
==========
stringency
==========
.. currentmodule:: bdgenomics.adam.stringency
.. autosummary::
   :toctree: _generate/

   STRICT
   LENIENT
   SILENT
"""

STRICT = 2
"""
    htsjdk.samtools.ValidationStringency.STRICT
"""
LENIENT = 1
"""
    htsjdk.samtools.ValidationStringency.LENIENT
"""
SILENT = 0
"""
    htsjdk.samtools.ValidationStringency.SILENT
"""

def _toJava(stringency, jvm):
    """
    Converts to an HTSJDK ValidationStringency enum.

    Should not be called from user code.

    :param bdgenomics.adam.stringency stringency: The desired stringency level.
    :param jvm: Py4j JVM handle.
    """

    if stringency is STRICT:
        return jvm.htsjdk.samtools.ValidationStringency.valueOf("STRICT")
    elif stringency is LENIENT:
        return jvm.htsjdk.samtools.ValidationStringency.valueOf("LENIENT")
    elif stringency is SILENT:
        return jvm.htsjdk.samtools.ValidationStringency.valueOf("SILENT")
    else:
        raise RuntimeError("Received %s. Stringency must be one of STRICT (%d), LENIENT (%d), or SILENT (%s)." % (stringency, STRICT, LENIENT, SILENT))
