#!/bin/bash
filename = "0.txt"
count = wc -l $filename
start = 0
fact = 2
co = 0
while $count > 0; do
	echo $count
	$start = $start + $fact
	head -n $fact $filename | tail -$fact > a$co.csv
	$co = $co+1
	$count = $count - $fact
	if [$count < $fact]
		then
		tail -$count > a$co.csv
		$count = 0
	fi
done; 