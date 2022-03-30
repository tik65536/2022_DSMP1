#!/bin/bash
cmd=$(echo $0 | sed 's|\./\(.*\)\.sh|\1|g')
python3 ./client.py $cmd $1
