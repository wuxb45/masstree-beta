#!/bin/bash
MHOME="/home/wuxb/work/c/keys"
TAG=${1}
runit ()
{
  export MTT_NR=${1}
  export MTT_FILE="${MHOME}/${2}"
  output="${TAG}-masstree-${1}-${2}.txt"
  #numactl -N 0 ./mttest -j  1 xkv 2>${output}
  for pset in 0; do
    export MTT_PSET=${pset}
    numactl -N 0 ./mttest -j 16 xkv 2>>${output}
  done
  #scp ${output} uhead:tmp/$(hostname)/
  sync
  sleep 1
}

runit   4502812 "dblp-ee-4502812.mmapkv"
runit   4077290 "dblp-title-4077290.mmapkv"
runit   4179061 "dblp-url-4179061.mmapkv"
runit  34703230 "addr-mx-34703230.mmapkv"
runit  58644196 "addr-br-58644196.mmapkv"
runit 142411471 "az-art-142411471.mmapkv"
runit 142411471 "az-rat-142411471.mmapkv"
runit 142411471 "az-tar-142411471.mmapkv"
runit 172942040 "addr-us-172942040.mmapkv"
runit 192678503 "m9urls-192678503.mmapkv"
