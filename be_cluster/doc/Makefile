plots = out_logfile400.png \
        out_logfile800.png \
        out_logfile1200.png

performance_report: performance_report.tex $(plots)
	pdflatex performance_report.tex

out_logfile400.png: logfile400
	./plot_dots.py logfile400

out_logfile800.png: logfile800
	./plot_dots.py logfile800

out_logfile1200.png: logfile1200
	./plot_dots.py logfile1200

clean:
	rm -rf out_logfile*.png performance_report.aux performance_report.log \
	performance_report.out performance_report.pdf

