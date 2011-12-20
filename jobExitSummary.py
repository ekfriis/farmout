#!/usr/bin/env python

'''

Take a condor (farmout) submit directory (or list of directories)
and summarize the exit code statuses for all the jobs

Example:
    cd /scratch/$LOGNAME/
    jobExitSummary.py MySubmitDir_Zjets
    jobExitSummary.py MySubmitDir_*

Author: Evan K. Friis, UW Madison

'''

import threading
import Queue
import sys
import os
import json
import glob

# Copied from https://twiki.cern.ch/twiki/bin/view/CMS/JobExitCodes
# on Dec. 6th 2011
JOB_EXIT_CODES = {
    '0' : 'SUCCESS',
    '-1' : 'Error return without specification',
    'NoLogFile' : 'Job has not completed',
    '1' : 'Hangup (POSIX)',
    '2' : 'Interrupt (ANSI)',
    '3' : 'unknown',
    '4' : 'Illegal instruction (ANSI)',
    '5' : 'Trace trap (POSIX)',
    '6' : 'Abort (ANSI) or IOT trap (4.2BSD)',
    '7' : 'BUS error (4.2BSD)',
    '8' : 'Floating point exception (ANSI)',
    '9' : 'killed, unblockable (POSIX) kill -9',
    '10' : 'User defined',
    '11' : 'segmentation violation (ANSI)',
    '12' : 'User defined',
    '15' : 'Termination (ANSI)',
    '24' : 'Soft CPU limit exceeded (4.2 BSD)',
    '25' : 'File size limit exceeded (4.2 BSD)',
    '29' : 'nondefined',
    '30' : 'Power failure restart (System V.)',
    '33' : 'nondefined',
    '64' : 'I/O error: cannot open data file (SEAL)',
    '65' : 'End of job from user application (CMSSW)',
    '66' : 'Application exception',
    '67' : 'unknown',
    '68' : 'unknown',
    '73' : 'Failed writing to read-only file system',
    '88' : 'unknown',
    '90' : 'Application exception',
    '100' : 'nondefined',
    '126' : 'unknown',
    '127' : 'Error while loading shared library',
    '129' : 'Hangup (POSIX)',
    '132' : 'Illegal instruction (ANSI)',
    '133' : 'Trace trap (POSIX)',
    '134' : 'Abort (ANSI) or IOT trap (4.2 BSD)',
    '135' : 'Bus error (4.2 BSD)',
    '136' : 'unknown',
    '137' : 'killed, unblockable (POSIX) kill -9',
    '138' : 'User defined',
    '139' : 'Segmentation violation (ANSI)',
    '140' : 'User defined',
    '143' : 'Termination (ANSI)',
    '152' : 'CPU limit exceeded (4.2 BSD)',
    '153' : 'File size limit exceeded (4.2 BSD)',
    '155' : 'Profiling alarm clock (4.2 BSD)',
    '251' : 'nondefined',
    '255' : 'nondefined',
    '256' : 'Application exception',
    '512' : 'nondefined',
    '2304' : 'nondefined',
    '0001' : 'Plug-in or message service initialization Exception',
    '7000' : 'Exception from command line processing',
    '7001' : 'Configuration File Not Found',
    '7002' : 'Configuration File Read Error',
    '8001' : 'Other CMS Exception',
    '8002' : 'std::exception (other than bad_alloc)',
    '8003' : 'Unknown Exception',
    '8004' : 'std::bad_alloc (memory exhaustion)',
    '8005' : 'Bad Exception Type (e.g throwing a string)',
    '8006' : 'ProductNotFound',
    '8007' : 'DictionaryNotFound',
    '8008' : 'InsertFailure',
    '8009' : 'Configuration',
    '8010' : 'LogicError',
    '8011' : 'UnimplementedFeature',
    '8012' : 'InvalidReference',
    '8013' : 'NullPointerError',
    '8014' : 'NoProductSpecified',
    '8015' : 'EventTimeout',
    '8016' : 'EventCorruption',
    '8017' : 'ScheduleExecutionFailure',
    '8018' : 'EventProcessorFailure',
    '8019' : 'FileInPathError',
    '8020' : 'FileOpenError (Likely a site error)',
    '8021' : 'FileReadError (May be a site error)',
    '8022' : 'FatalRootError',
    '8023' : 'MismatchedInputFiles',
    '8024' : 'ProductDoesNotSupportViews',
    '8025' : 'ProductDoesNotSupportPtr',
    '8026' : 'NotFound (something other than a product or dictionary not found)',
    '8027' : 'FormatIncompatibility',
    '10001' : 'LD_LIBRARY_PATH is not defined',
    '10002' : 'Failed to setup LCG_LD_LIBRAR_PATH',
    '10016' : 'OSG $WORKING_DIR could not be created',
    '10017' : 'OSG $WORKING_DIR could not be deleted',
    '10018' : 'OSG $WORKING_DIR could not be deleted',
    '10020' : 'Shell script cmsset_default.sh to setup cms environment is not found',
    '10021' : 'Failed to scram application project using the afs release area',
    '10022' : 'Failed to scram application project using CMS sw disribution on the LCG2',
    '10030' : 'middleware not identified',
    '10031' : 'Directory VO_CMS_SW_DIR not found',
    '10032' : 'Failed to source CMS Environment setup script such as cmssset_default.sh, grid system or site equivalent script',
    '10033' : 'Platform is incompatible with the scram version',
    '10034' : 'Required application version is not found at the site',
    '10035' : 'Scram Project Command Failed',
    '10036' : 'Scram Runtime Command Failed',
    '10037' : 'Failed to find cms_site_config file in software area',
    '10038' : 'Failed to find cms_site_catalogue.sh file in software area',
    '10039' : 'cms_site_catalogue.sh failed to provide the catalogue',
    '10040' : 'failed to generate cmsRun cfg file at runtime',
    '10041' : 'fail to find valid client for output stage out',
    '50110' : 'Executable file is not found',
    '50111' : 'Executable file has no exe permissions',
    '50112' : 'User executable shell file is not found',
    '50113' : 'Executable did not get enough arguments',
    '50114' : 'OSG $WORKING_DIR could not be deleted',
    '50115' : 'cmsRun did not produce a valid job report at runtime (often means cmsRun segfaulted)',
    '50116' : 'Could not determine exit code of cmsRun executable at runtime',
    '50117' : 'Could not update exit code in job report',
    '50513' : 'Failure to run SCRAM setup scripts',
    '50998' : 'Problem calculating file details (i.e. size, checksum etc)',
    '50999' : 'OSG $WORKING_DIR could not be deleted',
    '60300' : 'Either OutputSE or OutputSE_DIR not defined',
    '60301' : 'Neither zip nor tar exists',
    '60302' : 'Output file(s) not found',
    '60303' : 'File already exists on the SE',
    '60304' : 'Failed to create the summary file (production)',
    '60305' : 'Failed to create a zipped archive (production)',
    '60306' : 'Failed to copy and register output file',
    '60307' : 'Failed to copy an output file to the SE (sometimes caused by timeout issue).',
    '60308' : 'An output file was saved to fall back local SE after failing to copy to remote SE',
    '60309' : 'Failed to create an output directory in the catalogue',
    '60310' : 'Failed to register an output file in the catalogue',
    '60311' : 'Stage Out Failure in ProdAgent job',
    '60312' : 'Failed to get file TURL via lcg-lr command',
    '60313' : 'Failed to delete the output from the previous run via lcg-del command',
    '60314' : 'Failed to invoke ProdAgent StageOut Script',
    '60315' : 'ProdAgent StageOut initialisation error (Due to TFC, SITECONF etc)',
    '60316' : 'Failed to create a directory on the SE',
    '60317' : 'Forced timeout for stuck stage out',
    '60318' : 'Internal error in Crab cmscp.py stageout script',
    '60401' : 'Failure to assemble LFN in direct-to-merge by size (WMAgent)',
    '60402' : 'Failure to assemble LFN in direct-to-merge by event (WMAgent)',
    '60403' : 'Timeout during attempted file transfer - status unknown (WMAgent)',
    '60404' : 'Timeout during staging of log archives - status unknown (WMAgent)',
    '60405' : 'General failure to stage out log archives (WMAgent)',
    '60406' : 'Failure in staging in log files during log collection (WMAgent)',
    '60407' : 'Timeout in staging in log files during log collection (WMAgent)',
    '60408' : 'Failure to stage out of log files during log collection (WMAgent)',
    '60409' : 'Timeout in stage out of log files during log collection (WMAgent)',
    '60410' : 'Failure in deleting log files in log collection (WMAgent)',
    '60411' : 'Timeout in deleting log files in log collection (WMAgent)',
    '60451' : 'Output file lacked adler32 checksum (WMAgent)',
    '60999' : 'SG $WORKING_DIR could not be deleted',
    '70000' : 'Output_sandbox too big for WMS: output can not be retrieved',
    '70500' : 'Warning: problem with ModifyJobReport',
}

# Added by me from personal experience
JOB_EXIT_CODES['84'] = 'Input file is not a ROOT file'

def parse_file(filename):
    log_file = os.path.join(filename, 'report.log')
    if not os.path.exists(log_file):
        return { 'JobExitCode' : 'NoLogFile' }
    result = {}
    log_file = open(log_file, 'r')
    for line in log_file:
        if line.startswith('params : '):
            clean = line.strip().replace('params : ', '').replace("'", '"')
            info = json.loads(clean)
            result.update(info)
    return result

class ReportLogReader(threading.Thread):
    def __init__(self, files_to_read, results_queue):
        super(ReportLogReader, self).__init__()
        self.files_to_read = files_to_read
        self.results = results_queue

    def run(self):
        while True:
            try:
                file_to_read = self.files_to_read.get()
                if file_to_read is None:
                    break
                result = parse_file(file_to_read)
                self.results.put( (result, file_to_read) )
            except KeyboardInterrupt:
                sys.exit(2)
            finally:
                self.files_to_read.task_done()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: jobExitSummary [list of directories]"
        sys.exit(1)

    folder_globs = sys.argv[1:]

    folders_to_read = []
    for folder_glob in folder_globs:
        folders_to_read.extend(glob.glob(folder_glob))

    for folder in folders_to_read:
        submit_dirs_to_read = list(
            x for x in glob.glob(os.path.join(folder, '*'))
            if os.path.isdir(x))

        input_queue = Queue.Queue()
        output_queue = Queue.Queue()

        workers = 10
        workers = [ ReportLogReader(input_queue, output_queue)
                   for i in range(workers) ]
        map(lambda x: x.start(), workers)

        print ""
        print "==================================================================="
        print "Submit directory: %s" % folder
        print "==================================================================="
        print "Starting examination of %i submit dirs" % len(submit_dirs_to_read)
        for dir in submit_dirs_to_read:
            input_queue.put(dir)

        # Poison pills
        for worker in workers:
            input_queue.put(None)

        print "Waiting for jobs to finish"
        input_queue.join()

        outputs = []
        while not output_queue.empty():
            outputs.append(output_queue.get()[0])

        jobExitCodes = {}
        for output in outputs:
            code = 'Unknown'
            if 'JobExitCode' in output:
                code = output['JobExitCode']
            count = jobExitCodes.get(code, 0)
            count += 1
            jobExitCodes[code] = count

        print "%-10s %5s   %-40s" % ("Code:", "N", "Description")
        print "-------------------------------------------------------------------"
        for k, v in jobExitCodes.iteritems():
            print "%-10s %5i   %-40s" % (k, v, JOB_EXIT_CODES.get(k, 'Unknown error code!'))
