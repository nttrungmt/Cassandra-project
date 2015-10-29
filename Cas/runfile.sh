#!/bin/bash
db=D8
node=1
count=20
echo $count
start=0
echo `javac -classpath cassandra-java-driver-2.0.2/cassandra-driver-core-2.0.2.jar:. ThreadClient.java`
while (( start!=count ))
do
	start=$((start+1))
	echo `java -classpath cassandra-java-driver-2.0.2/*:cassandra-java-driver-2.0.2/lib/*:. ThreadClient $db $node $count > o$start.txt`
	echo $start
done
