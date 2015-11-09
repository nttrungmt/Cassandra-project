#!/bin/bash
db=D8
count=1
end=100
echo `rm -rf *.class; javac -classpath driver/cassandra-driver-core-2.1.0.jar:. ThreadClient.java`
while (( count!=end ))
do
	echo $count
	echo `java -classpath driver/*:driver/lib/*:. ThreadClient $db $count > o.txt 2>e$count.txt`
	count=$((count+1))
done
