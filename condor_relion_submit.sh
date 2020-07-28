#!/bin/sh
# HTCondor submission script for RELION 3.0
#
####
#
# In the "Running" tab, set (***changes required):
#     Number of MPI procs: 1 (Do not increase this!)
#     Number of threads: (if available) Same as "Minimum dedicated cores per node"
# *** Submit to queue?: Yes
#     Queue name: Leave at default
# *** Queue submit command: sh
# *** Standard submission script: condor_relion_submit.sh
#     Minimum dedicated cores per node: Leave this at 1, unless the "Number of
#         threads" option is available and you want to increase the number of
#         CPU cores (and threads) to have HTCondor request and use per job.
#         Setting this higher than 1 will likely increase jobs' idle times.
#
####

# Template variables set by relion
MPINODES="XXXmpinodesXXX"
THREADS="XXXthreadsXXX"
CORES="XXXcoresXXX"
DEDICATED="XXXdedicatedXXX"
NODES="XXXnodesXXX"
NAME="XXXnameXXX"
ERRFILE="XXXerrfileXXX"
OUTFILE="XXXoutfileXXX"
QUEUE="XXXqueueXXX"
COMMAND="XXXcommandXXX"

# Extra variables can be defined by setting
# RELION_QSUB_EXTRA<n> environment variables
# before running relion.
#EXTRA1="XXXextra1XXX"
#EXTRA2="XXXextra2XXX"
#EXTRA3="XXXextra3XXX"
# etc.

# Check input
if [ "$MPINODES" -gt "1" ]; then
    err='Setting "Number of MPI procs" > 1 is not supported by this HTCondor scheduling method'
    echo $err >&2
    echo $err >> $ERRFILE
    exit 1
fi

# Run the job submission script
exec ./condor_relion_submit.py --command="$COMMAND" --threads="$THREADS" --dedicated="$DEDICATED" --outfile="$OUTFILE" --errfile="$ERRFILE"
