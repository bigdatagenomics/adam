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

import numpy as np
import matplotlib.pyplot as plt

# sizes in MB
gff_sizes  = [  29.9,       36, 392.2]
gff_labels = ['ADAM', 'GFF3.GZ', 'GFF3']

# sizes in GB
bed_sizes  = [   9.1,       16, 118.9]
bed_labels = ['ADAM', 'BED.GZ', 'BED']

# sizes in GB
bam_sizes  = [    69,   95.1, 119.1]
bam_labels = ['CRAM', 'ADAM', 'BAM']

# sizes in GB
vcf_sizes  = [      19,     21,   807]
vcf_labels = ['VCF.GZ', 'ADAM', 'VCF']

def plot(sizes, labels, filename, title, unit='GB'):

    ind = np.arange(len(sizes))
    width = 0.35

    fig, ax = plt.subplots()
    rects = ax.bar(ind - (width / 2.0), sizes, width)

    (ymin, ymax) = plt.ylim()
    plt.ylim(ymin, ymax + 10)
    ax.set_ylabel('File size (%s)' % unit)
    ax.set_title(title)
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)

    for (rect, size) in zip(rects, sizes):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2.0, 1.01 * height,
                '%d %s' % (size, unit),
                ha='center', va='bottom')
    
    fig.savefig(filename)

plot(gff_sizes, gff_labels, 'gff.pdf', 'File sizes for ENSEMBL GRCh38 GTF', unit='MB')
plot(bed_sizes, bed_labels, 'bed.pdf', 'File sizes for WGS Coverage BED')
plot(bam_sizes, bam_labels, 'bam.pdf', 'File sizes for 60x WGS BAM')
plot(vcf_sizes, vcf_labels, 'vcf.pdf', 'File sizes for 1,000 Genomes VCF')
