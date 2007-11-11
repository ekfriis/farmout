#!/bin/sh

# This script is used to report farmout jobs to the CMS dashboard.
# Users should not need to call this script directly.

if [ "$CMS_DASHBOARD_REPORTER" = "" ]; then
  echo "No CMS_DASHBOARD_REPORTER found in environment. "\
       "Dashboard will not be updated."
  exit 1
fi

if ! [ -d "$CMS_DASHBOARD_REPORTER" ]; then
  echo "Cannot access $CMS_DASHBOARD_REPORTER. "\
       "Dashboard will not be updated."
  exit 1
fi

check_required_dashboard_args() {
    # Check that required parameters (specified as arguments to this function)
    # are defined in the environment with the prefix "dboard_".
    # Also check for the special parameters taskId and jobId.
    for param in taskId jobId $@; do
        dboard_param=dboard_${param}
        value=${!dboard_param}
        if [ -z "${value}" ]; then
            echo "${dboard_param} is not defined in environment. "\
                 "Dashboard will not be updated."
            exit 1
        fi
    done
}

# Produces dboard_args.
# The arguments to this function are the names of the parameters that
# should be extracted from the environment and inserted into the command-line
# arguments to the dashboard reporting tool.
build_dashboard_args() {

    # first the special monitoring ids
    dboard_args="MonitorID='${dboard_taskId}' MonitorJobID='${dboard_jobId}'"

    for param in $@; do
        dboard_param=dboard_${param}
        value=${!dboard_param}
        if ! [ -z "${value}" ]; then
            dboard_args="${dboard_args} ${param}='${value}'"
        fi
    done
}

report_to_dashboard() {
    # We have to use 'eval' here because the arguments contain
    # quotes around values that contain spaces, and we need these
    # quotes to be interpreted by the shell.
    eval python ${CMS_DASHBOARD_REPORTER}/report.py "$1"
}

report_task_meta() {
    # This TaskMeta is something CRAB does.  I have found no documentation
    # of it.
    dboard_jobIdSave="${dboard_jobId}"
    dboard_jobId="TaskMeta"

    required_args="taskId jobId application exe tool GridName scheduler taskType vo user"
    optional_args="dataset owner JSToolVersion tool_ui"
    check_required_dashboard_args ${required_args}
    build_dashboard_args ${required_args} ${optional_args}
    report_to_dashboard "$dboard_args"

    dboard_jobId="${dboard_jobIdSave}"
}

report_submission() {
    report_task_meta

    required_args="taskId jobId sid application exe tool GridName scheduler taskType vo user"
    optional_args="dataset owner JSToolVersion tool_ui"
    check_required_dashboard_args ${required_args}
    build_dashboard_args ${required_args} ${optional_args}
    report_to_dashboard "$dboard_args"
}

report_execution() {
    dboard_SyncGridJobId=${dboard_sid}
    dboard_SyncGridName=${dboard_GridName}

    required_args="SyncGridJobId SyncCE"
    optional_args="GridFlavour"
    check_required_dashboard_args ${required_args}
    build_dashboard_args ${required_args} ${optional_args}
    report_to_dashboard "$dboard_args"
}

report_completion() {
    dboard_ExeEnd=${dboard_exe}

    if [ "${dboard_ExeExitCode}" = "" ]; then
        dboard_ExeExitCode=${dboard_JobExitCode}
    fi
    if [ "${dboard_JobExitCode}" = "" ]; then
        dboard_JobExitCode=${dboard_ExeExitCode}
    fi

    required_args="ExeTime ExeExitCode"
    optional_args=""
    check_required_dashboard_args ${required_args}
    build_dashboard_args ${required_args} ${optional_args}
    report_to_dashboard "$dboard_args"

    required_args="JobExitCode"
    optional_args=""
    check_required_dashboard_args ${required_args}
    build_dashboard_args ${required_args} ${optional_args}
    report_to_dashboard "$dboard_args"
}

if [ "$dboard_GridName" = "" ]; then
    dboard_GridName=`grid-proxy-info -subject 2>/dev/null`
fi

dboard_vo=${dboard_vo:-cms}

if [ "${dboard_sid:0:8}" != "https://" ]; then
    # See note below about jobId needing "https://" at the beginning.
    dboard_sid="https://${dboard_sid}"
fi

# job id must be of the form "[number]_[sid]" or
# "[number]_https://[sid] if sid does not begin with https.  I found
# this by trial and error, and by looking at how CRAB does things.
# Without this, the completion time of the job never gets updated.
# dboard_jobId="${dboard_jobId}_${dboard_sid}"

task=$1

if [ "$task" = "" ]; then
    echo "You must specify what option to use for reporting to the dashboard."
fi

if [ "$task" = "submission" ] || [ "$task" = "all" ]; then
    report_submission
fi

if [ "$task" = "execution" ] || [ "$task" = "all" ]; then
    report_execution
fi

if [ "$task" = "completion" ] || [ "$task" = "all" ]; then
    report_completion
fi
