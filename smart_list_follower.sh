#!/bin/bash
COUNTER=0  
while [  $COUNTER -lt 10 ]; do  
echo The counter is $COUNTER  
let COUNTER=COUNTER+1   
time=`date`
echo $time
python3 ./smart_list_follower.py -R|tee "$time.log"
done  

