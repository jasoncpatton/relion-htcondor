# relion-htcondor

This repository contains a set of scripts
that can be used with RELION
to submit a batch job to HTCondor
while running RELION on a submit node.

To use these scripts, place
`condor_relion_submit.sh`,
`condor_relion_submit.py`, and
`condor_relion_wrapper.sh`
in your RELION project's directory.
Then, for supported tasks,
in the "Running" tab, set:
* Number of MPI procs: `1`
* Number of threads (if listed): `1` (see note)
* Submit to queue?: `Yes`
* Queue name: (not used, can be set to anything)
* Standard submission script: `condor_relion_submit.sh`
* Minimum dedicated cores per node: `1` (see note)

Note: When "Number of threads" is an option, you may increase "Number of threads" and "Minimum dedicated cores per node" (both must be set to the same value) to have HTCondor request and use more CPU cores. However, a job that requests more than 1 CPU may sit idle longer in the HTCondor job queue.

Current supported tasks:
* MotionCorr
* CtfFind

## Implementation details

`condor_relion_submit.sh`
reads templated variables set by RELION.
It then executes `condor_relion_submit.py`
using the values inserted into those templated variables.

`condor_relion_submit.py` splits the requested task (if possible) into multiple HTCondor jobs,
queues the job(s), and
cleans up the job(s)'s outputs.
Specifically, it:
1. Adds required library files needed by any executables in a task to each HTCondor job's set of transferred input files, along with the executables themselves.
2. When a task is splittable, splits the input starfile to multiple starfiles, one per input movie file.
3. Adds the input movie(s) and starfile to each HTCondor job's set of transferred input files.
4. Writes and submits a DAG that runs `condor_relion_wrapper.sh` for each HTCondor job, and that runs `condor_relion_submit.py` as a post script after all HTCondor jobs have completed.
5. When run as a post script, merges all output starfiles into a single starfile and marks the task as complete.

`condor_relion_wrapper.sh`
Runs when HTCondor starts a job on an execute node.
Sets up the job environment (puts library files in the load path) and
runs the requested RELION command using the starfile and movie files
that were transferred to the execute node.
