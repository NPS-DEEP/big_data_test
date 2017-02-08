drawings = three_tier_bimap.pdf

.SUFFIXES: .svg .pdf

.svg.pdf:
	inkscape --without-gui -D --export-pdf=$@ $<

fm_design: fm_design.tex $(drawings)
	pdflatex fm_design.tex

clean:
	rm -rf out_logfile*.png fm_design.aux fm_design.log \
	fm_design.out *.pdf

