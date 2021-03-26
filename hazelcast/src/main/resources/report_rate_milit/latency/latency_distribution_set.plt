set datafile separator ","
set title 'Latency distribution' noenhanced
set terminal png size 1280,1024
set grid
unset xtics
set ylabel 'Latency (μs)'
set logscale x
set key top left
set style line 1 lt 1 lw 3 pt 3 linecolor rgb "red"
set output '/Users/ahmet.mircik/simulator/tests/report_3M/latency/latency_distribution_set.png'
plot '/Users/ahmet.mircik/simulator/hazelcast-simulator-0.13-SNAPSHOT/bin/xlabels.csv' notitle with labels center offset 0, 1.5 point,\
   "/Users/ahmet.mircik/simulator/tests/report_3M/data/latency_distribution_set_1.data" using 1:2 title "3_11_4_3M" noenhanced lt rgb "red" with lines, \
   "/Users/ahmet.mircik/simulator/tests/report_3M/data/latency_distribution_set_2.data" using 1:2 title "4_1_1_3M" noenhanced lt rgb "blue" with lines, \
