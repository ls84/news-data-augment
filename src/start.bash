#!/bin/bash
echo "wait for 120 secs"
sleep 120
n=0
until [ $n -ge 60 ]
do
  /src/main.py && break
  n=$[$n+1] 
  sleep 60
done
