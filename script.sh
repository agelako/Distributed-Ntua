#! /bin/bash
filename="server_""$1""_""$2"".py"
cp slaveserver.py $filename
python3 $filename $1 $2

