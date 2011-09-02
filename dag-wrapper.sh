#!/bin/sh

JOBID=$1
RETURN=$2
shift 2

# The rest of the arguments are name=value pairs.
export $*

cat <<EOF | mail -s "farmout DAG returned ${RETURN}" ${USER}
Hello-

The following farmout DAG workflow on $(hostname) returned ${RETURN}:
    
    ${OUTPUT_DAG_FILE}

You can find the final output in:

    ${OUTPUT_DIR}

The status of all of the jobs in this workflow can be found here:

    ${OUTPUT_DAG_FILE}.status

If your DAG workflow exited with a status other than 0, you can resubmit only
the jobs that failed (and their dependencies) with the following command:
    
    \$ farmoutAnalysisJobs --rescue-dag-file ${OUTPUT_DAG_FILE}

If some of the failed jobs are unrecoverable, you'll need to edit the DAG
workflow file and mark the unrecoverable jobs 'NOOP'. If the jobs that depend on
the unrecoverable job require its output, you may have to modify the dependent
jobs' submit files as well.

For more information about DAG workflows, please see the Condor manual:
    
    http://www.cs.wisc.edu/condor/manual/v7.6/2_10DAGMan_Applications.html

For questions about farmout and its DAG support or for help debugging your jobs,
please contact help@hep.wisc.edu.

Thanks!
EOF

exit ${RETURN}
