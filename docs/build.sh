#/usr/bin/env bash

git_version=$(git log -1 --pretty=format:%H)

output_dir="output"
pdf_output="$output_dir/ADAM_${git_version}.pdf"
html_output="$output_dir/ADAM_${git_version}.html"
md_output="$output_dir/ADAM_${git_version}.rst"
date_str=$(date '+%Y-%m-%d')

mkdir -p ${output_dir}

title="ADAM User Guide"
date="$date_str git:$git_version"
author="http://bdgenomics.org/"
highlight_style="tango"

which pandoc >/dev/null 2>&1
if [ $? -ne "0" ]; then
	echo "WARNING! Pandoc not found on path. Documentation will not be generated!"
	exit 0
fi

# Generate a PDF of the docs
pandoc -N -t rst \
       --mathjax \
--filter pandoc-citeproc \
--highlight-style "$highlight_style" \
--variable mainfont="Georgia" \
--variable sansfont="Arial" \
--variable monofont="Andale Mono" \
--variable fontsize=10pt \
--variable version=$git_version \
--variable listings=true \
--variable title="$title" \
--variable date="$date" \
--variable author="$author" \
--toc \
--bibliography=source/bibliography.bib \
source/*.md -s -S -o $md_output

# Generate a PDF of the docs
pandoc -N --template=template.tex \
--filter pandoc-citeproc \
--highlight-style "$highlight_style" \
--variable mainfont="Georgia" \
--variable sansfont="Arial" \
--variable monofont="Andale Mono" \
--variable fontsize=10pt \
--variable version=$git_version \
--variable listings=true \
--variable title="$title" \
--variable date="$date" \
--variable author="$author" \
--toc \
--bibliography=source/bibliography.bib \
source/*.md -s -S -o $pdf_output

# Generate HTML of the docs
pandoc source/*.md -H style.css -s -S --toc \
--mathjax \
--filter pandoc-citeproc \
--bibliography=source/bibliography.bib \
--highlight-style "$highlight_style" \
--variable title="$title" \
--variable date="$date" \
--variable author="$author" \
-o $html_output
