#!/bin/sh

. /etc/profile
module load GCC/8.3.0
module load OpenMPI/3.1.4-GCC-8.3.0

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.

$@
