#!/bin/bash

if [ $# -ne 3 ]; 
then
    echo 'Usage: debug.sh <test_name> <timeout(s)> <count>.'
    exit 1
fi

i=0
timeout=$2
count=$3

while (( $i < $count )) 
do
    `go test -count=1 -timeout=${timeout}s $(pwd) -run $1 -v > out`
    line=`tail -1 out`
	let i+=1

    if [[ $line == ok* ]]; then
        echo "第${i}次成功了...."
        rm -f out
        continue
    fi
    
    mv out error.log
    echo "第${i}次失败了，日志位于：error.log."
    break
done

if [[ $i -eq $count ]]; then
    echo "执行了${count}次，全都成功了..."
fi
