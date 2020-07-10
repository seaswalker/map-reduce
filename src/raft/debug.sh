#!/bin/zsh

if [ $# -ne 2 ]; 
then
    echo 'Usage: debug.sh <test_name> <count>.'
    exit 1
fi

i=0
count=$2
while (( $i < $count )) 
do
    `go test -count=1 -run $1 > out`
    line=`tail -1 out`
    let i+=1

    if [[ $line == ok* ]]; then
        echo "第$i次成功了...." 
        rm -f out
        continue
    fi
    
    mv out error.log
    echo "第$i次失败了，日志位于：error.log."
    break
done

if [[ $i -eq $count ]]; then
    echo "执行了$count次，全都成功了..."
fi
