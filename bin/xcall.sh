#! /bin/bash
 
for i in node1 node2 node3
do
    echo --------- $i ----------
    ssh $i "$*"
done
