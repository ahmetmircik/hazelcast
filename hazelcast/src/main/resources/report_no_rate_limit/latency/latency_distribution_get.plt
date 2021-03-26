set datafile separator ","
set title 'Latency distribution' noenhanced
set terminal png size 1280,1024
set grid
unset xtics
set ylabel 'Latency (μs)'
set logscale x
set key top left
set style line 1 lt 1 lw 3 pt 3 linecolor rgb "red"
set output '/Users/ahmet.mircik/simulator/tests/report_3M_no_rate_limit/latency/latency_distribution_get.png'
plot '/Users/ahmet.mircik/simulator/hazelcast-simulator-0.13-SNAPSHOT/bin/xlabels.csv' notitle with labels center offset 0, 1.5 point,\
   "/Users/ahmet.mircik/simulator/tests/report_3M_no_rate_limit/data/latency_distribution_get_1.data" using 1:2 title "3_11_4_3M_no_rate_limit" noenhanced lt rgb "red" with lines, \
   "/Users/ahmet.mircik/simulator/tests/report_3M_no_rate_limit/data/latency_distribution_get_2.data" using 1:2 title "4_1_1_3M_no_rate_limit" noenhanced lt rgb "blue" with lines, \
