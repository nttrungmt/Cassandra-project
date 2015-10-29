#!/bin/bash
filename="itemstockmaster.csv"
wc -l $filename > temp
read count zzz < temp
count=$((count+1))
echo $count
start=0
fact=1000000
co=0
while (( count!=0 ))
do
	start=$((start+fact))
	echo `head -n $start $filename | tail -$fact > itemstockmaster$co.csv`
	co=$((co+1))
	echo $co
	count=$((count-fact))
	echo $count
	if (( count < fact ))
		then
		echo `tail -$count $filename > itemstockmaster$co.csv`
		count=0
	fi
done


filename="ordercsv.csv"
wc -l $filename > temp
read count zzz < temp
count=$((count+1))
echo $count
start=0
co=0
while (( count!=0 ))
do
	start=$((start+fact))
	echo `head -n $start $filename | tail -$fact > order$co.csv`
	co=$((co+1))
	echo $co
	count=$((count-fact))
	echo $count
	if (( count < fact ))
		then
		echo `tail -$count $filename > order$co.csv`
		count=0
	fi
done

filename="stocks.csv"
wc -l $filename > temp
read count zzz < temp
count=$((count+1))
echo $count
start=0
co=0
while (( count!=0 ))
do
	start=$((start+fact))
	echo `head -n $start $filename | tail -$fact > stocks$co.csv`
	co=$((co+1))
	echo $co
	count=$((count-fact))
	echo $count
	if (( count < fact ))
		then
		echo `tail -$count $filename > stocks$co.csv`
		count=0
	fi
done
