set title 'Memory Free' noenhanced
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
set ylabel 'Memory Free'
set format y '%.1s%cB'
set output '/Users/ahmet.mircik/simulator/tests/report_3M_rate_limit_fixed_io/dstat/memory_free_A3.png'
plot \
   '/Users/ahmet.mircik/simulator/tests/report_3M_rate_limit_fixed_io/data/memory_free_A3_1.data' using (t0(timecolumn(1))):2 title "4_1_1_3M" noenhanced lt rgb "red", \
   '/Users/ahmet.mircik/simulator/tests/report_3M_rate_limit_fixed_io/data/memory_free_A3_2.data' using (t0(timecolumn(1))):2 title "4_1_1_3M_fixed_io" noenhanced lt rgb "blue", \
