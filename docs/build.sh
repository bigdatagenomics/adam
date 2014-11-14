#/usr/bin/env bash

git_version=$(git rev-parse --short HEAD)
output_dir="output"
pdf_output="$output_dir/ADAM_v$git_version.pdf"
html_output="$output_dir/ADAM_v$git_version.html"

# Generate a PDF of the docs
pandoc -N --template=template.tex \
--variable mainfont="Georgia" \
--variable sansfont="Arial" \
--variable monofont="Andale Mono" \
--variable fontsize=10pt \
--variable version=$git_version \
--variable listings=true \
source/*.md -s -S --toc -o $pdf_output \
--latex-engine=lualatex

# Generate HTML of the docs
pandoc source/*.md -s -S --toc -o $html_output
