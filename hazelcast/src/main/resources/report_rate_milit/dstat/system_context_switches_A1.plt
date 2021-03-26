set title 'System Context Switches' noenhanced
set style data lines
set datafile separator ","
set terminal png size 1280,1024
set grid
set key below
set xdata time
set timefmt "%s"
offset = 0
t0(x)=(offset=($0==0) ? x : offset, x - offset)
set xlabel 'Time minutes:seconds'
set ylabel 'System Context Switches/sec'
set output '/Users/ahmet.mircik/simulator/tests/report_share/dstat/system_context_switches_A1.png'
plot \
   '/Users/ahmet.mircik/simulator/tests/report_share/data/system_context_switches_A1_1.data' using (t0(timecolumn(1))):2 title "3_11_4_3M" noenhanced lt rgb "red", \
   '/Users/ahmet.mircik/simulator/tests/report_share/data/system_context_switches_A1_2.data' using (t0(timecolumn(1))):2 title "4_1_1_3M_fixed_io" noenhanced lt rgb "blue", \
