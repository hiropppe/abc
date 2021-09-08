#!/bin/bash
 
if [ "$1" = 'heritrix' ]; then
    ./heritrix-3.4.0-SNAPSHOT/bin/heritrix -a admin:admin -b 0.0.0.0 -j /heritrix/jobs
else
    exec "$@"
fi
