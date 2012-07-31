#!/bin/env python

'''

Parse a set of job report xml files to determine skim efficiency.

Author: Evan K. Friis, UW Madison

'''

import copy
import glob
import json
import os
import Queue
import subprocess
import sys
import threading

from itertools import chain
from xml.etree.ElementTree import ElementTree

try:
    import RecoLuminosity.LumiDB.argparse as argparse
except ImportError:
    argparse = None

class RunLumiCorruptionError(Exception):
    '''
    Exception raised when the output Run-Lumi content of a job does not match
    the input.
    '''
    def __init__(self, job_report, input_run_info, output_run_info):
        super(RunLumiCorruptionError, self).__init__(
            job_report, input_run_info, output_run_info)

        self.job_report = job_report
        self.input_run_info = input_run_info
        self.output_run_info = output_run_info
        self.missing_run_lumis = input_run_info - output_run_info
        self.extra_lumis = output_run_info - input_run_info

        self.n_missing_run_lumis = len(self.missing_run_lumis)
        self.n_extra_lumis = len(self.extra_lumis)
        self.extra_lumis = " ".join("%i:%i" % x for x in self.extra_lumis),
        self.missing_run_lumis = " ".join("%i:%i" % x for x in self.missing_run_lumis),

    def __str__(self):
        return ("In job report %(job_report)s:\n"
                "The following %(n_missing_run_lumis)i run-lumi pairs are in the input,"
                    " but not the output.\n"
                "%(missing_run_lumis)s\n"
                "The following %(n_extra_lumis)i run-lumi pairs are in the output,"
                    " but not the input.\n"
                "%(extra_lumis)s\n") % vars(self)

class JobOverlapError(Exception):
    '''
    Exception raised when two job reports have processed the same run-lumi
    '''
    def __init__(self, current, to_merge):
        super(JobOverlapError, super).__init__(current, to_merge)
        self.current = current
        self.to_merge = to_merge
        self.input_overlap = current['input_run_lumis'] - to_merge['input_run_lumis']

        self.job_reports = to_merge['job_reports']
        self.overlap_lumis = " ".join("%i:%i"  % x for x in input_overlap)

    def __str__(self):
        return ("Error when merging job report: %(job_reports)s\n"
                "The following run-lumi pairs are already in the merged"
                    " job report:\n%(overlap_lumis)s\n") % vars(self)

def parse_job_report(fd):
    ''' Return a dictionary containing the input and output parameters
    '''
    filename = fd.name
    output = {
        # Input
        'job_reports' : [filename],
        'input_files' : [],
        'input_run_lumis' : set([]),
        'events_read' : 0,
        # Output
        'output_files' : [],
        'output_run_lumis' : set([]),
        'events_written' : 0,
        'ok' : True,
        'bad_files' : [],
    }

    tree = ElementTree()
    try:
        tree.parse(fd)
    except:
        output['ok'] = False
        output['bad_files'] = [filename]
        return output

    # Loop over input files
    for input_file in tree.findall('InputFile'):
        filename = input_file.find('PFN').text
        output['input_files'].append(filename)

        events = int(input_file.find('EventsRead').text)
        output['events_read'] += events

        runs = input_file.find('Runs')
        for run in runs.findall('Run'):
            run_number = int(run.get('ID'))
            # Get the lumi list for this run
            lumi_list = output['input_run_lumis']
            lumi_list.update(
                (run_number, int(lumi.get('ID')))
                for lumi in run.findall('LumiSection')
            )

    # Loop over output files
    for output_file in tree.findall('File'):
        filename = output_file.find('PFN').text
        output['output_files'].append(filename)

        events = int(output_file.find('TotalEvents').text)
        output['events_written'] += events

        runs = output_file.find('Runs')
        for run in runs.findall('Run'):
            run_number = int(run.get('ID'))
            # Get the lumi list for this run
            lumi_list = output['output_run_lumis']
            lumi_list.update(
                (run_number, int(lumi.get('ID')))
                for lumi in run.findall('LumiSection')
            )

    # Do some sanity checks
    #if output['output_run_lumis'] != output['input_run_lumis']:
        #raise RunLumiCorruptionError(
            #filename, output['input_run_lumis'], output['output_run_lumis'])

    # Return info dictionary
    return output

class JobReportReader(threading.Thread):
    '''
    JobReportReader

    Takes file names from the files_to_read_queue, parses them, and add the
    dictionary to the result_queue.

    The input and output .root files extracted from the job reports are put
    in the input and output file queues.

    '''

    def __init__(self, files_to_read, results_queue):
        super(JobReportReader, self).__init__()
        self.files_to_read = files_to_read
        self.results_queue = results_queue

    def run(self):
        while True:
            try:
                to_parse = self.files_to_read.get()
                # Poison pill to be done.
                if to_parse is None:
                    break
                with open(to_parse, 'r') as f:
                    result = parse_job_report(f)
                # Store the result in the output queue
                self.results_queue.put(result)
            finally:
                self.files_to_read.task_done()

class JobReportMerger(threading.Thread):
    '''
    JobReportMerger

    Takes merged job report dictionaries out of the results queue and merges
    them.
    '''
    def __init__(self, to_merge_queue, target_count,
                 log=sys.stdout, verbose=False, overlaps_ok = False):
        super(JobReportMerger, self).__init__()
        self.to_merge_queue = to_merge_queue
        self.log = log
        self.verbose = verbose
        self.result = None
        self.count = 0
        self.target_count = target_count
        self.overlaps_ok = overlaps_ok
        if verbose:
            self.log.write("Merging %i job reports\n" % self.target_count)
            self.log.flush()

    def merge_job_report(self, to_merge):
        """ Merge job report into the owned result """
        # If we haven't defined our local copy, do it now.
        if self.result is None:
            self.result = copy.copy(to_merge)
            return
        merge_into = self.result
        for key, value in merge_into.iteritems():
            value_to_merge = to_merge[key]
            # Check for overlaps in any of the sets
            if isinstance(value, set):
                overlaps = value & value_to_merge
                if overlaps and not self.overlaps_ok:
                    raise JobOverlapError(merge_into, to_merge)
                merge_into[key] |= value_to_merge
            elif isinstance(value, (int, list)):
                merge_into[key] += value_to_merge
            elif isinstance(value, bool):
                merge_into[key] = merge_into[key] and value_to_merge
            else:
                raise ValueError("failed to merge %s" % type(value))

    def write_status_message(self, add_carriage_return=True):
        fraction = self.count*100./self.target_count
        output = "Merged %5i/%i (%0.0f%%) job reports" % (
            self.count, self.target_count, fraction)
        if add_carriage_return:
            self.log.write('\r')
        self.log.write(output)
        self.log.flush()

    def run(self):
        while True:
            try:
                report_to_merge = self.to_merge_queue.get()
                if report_to_merge is None:
                    if self.verbose:
                        self.write_status_message(True)
                        self.log.write('\n')
                        self.log.flush()
                    break
                self.merge_job_report(report_to_merge)
                if self.verbose:
                    self.write_status_message()
                self.count += 1
            finally:
                self.to_merge_queue.task_done()


def parse_job_reports(files_to_parse, reader_workers=5, verbose=False,
                      overlaps_ok=False):
    files_to_parse = list(files_to_parse)
    for to_parse in files_to_parse:
        if os.path.isdir(to_parse):
            sys.stderr.write(
                "You passed a directory.  All of the input paths should"
                " be job report .xml files.  Ex: mySubmitDir/*/*.xml\n"
            )
            sys.exit(2)

    work_queue = Queue.Queue()
    parsed_results_queue = Queue.Queue()
    # Build the readers
    readers = [JobReportReader(work_queue, parsed_results_queue)
               for i in range(reader_workers)]
    # Build the merger
    merger = JobReportMerger(parsed_results_queue, len(files_to_parse),
                            verbose=verbose, overlaps_ok=overlaps_ok)
    # Start all the reader threads
    map(lambda x: x.start(), readers)
    merger.start()
    for file in files_to_parse:
        work_queue.put(file)
    # Add poison pills to stop readers
    for reader in readers:
        work_queue.put(None)
    # Wait for all results to be in the results queue
    work_queue.join()
    # Add poison pill to stop merger
    parsed_results_queue.put(None)
    # Block until all results are merged
    parsed_results_queue.join()
    return merger.result

class FileSizeWorker(threading.Thread):
    '''
    Class that looks up file sizes from a PFN
    '''
    def __init__(self, file_queue, result_queue):
        super(FileSizeWorker, self).__init__()
        self.queue = file_queue
        self.results_queue = result_queue

    @staticmethod
    def get_file_size(pfn):
        file_util_call = subprocess.Popen(
            ['edmFileUtil', '-j', pfn],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = file_util_call.communicate()
        size = -1
        if file_util_call.returncode == 0:
            file_dict = json.loads(stdout)[0]
            size = file_dict['bytes']
        return size

    def run(self):
        while True:
            try:
                file_to_query = self.queue.get()
                if file_to_query is None:
                    break
                size = self.get_file_size(file_to_query)
                self.results_queue.put((file_to_query, size))
            finally:
                self.queue.task_done()

def get_items(queue):
    output = []
    while not queue.empty():
        output.append(queue.get())
    return output

def get_file_sizes(input_pfn_list, output_pfn_list, workers=20):
    input_file_input_queue = Queue.Queue()
    input_file_results_queue = Queue.Queue()

    output_file_input_queue = Queue.Queue()
    output_file_results_queue = Queue.Queue()

    input_workers = [FileSizeWorker(
        input_file_input_queue, input_file_results_queue)
        for i in range(workers)]

    output_workers = [FileSizeWorker(
        output_file_input_queue, output_file_results_queue)
        for i in range(workers)]

    map(lambda x: x.start(), input_workers + output_workers)
    # Load the files
    for file in input_pfn_list:
        input_file_input_queue.put(file)
    for file in output_pfn_list:
        output_file_input_queue.put(file)
    # Add poison pills
    for worker in input_workers:
        input_file_input_queue.put(None)
    for worker in output_workers:
        output_file_input_queue.put(None)
    input_file_input_queue.join()
    output_file_input_queue.join()
    return (get_items(input_file_results_queue),
            get_items(output_file_results_queue))

def group_by_run(sorted_run_lumis):
    '''
    Generate a list of lists run-lumi tuples, grouped by run
    Example:
    >>> run_lumis = [(100, 1), (100, 2), (150, 1), (150, 2), (150, 8)]
    >>> list(group_by_run(run_lumis))
    [(100, [1, 2]), (150, [1, 2, 8])]

    '''
    current_run = None
    output = []
    for run, lumi in sorted_run_lumis:
        if current_run is None or run == current_run:
            output.append(lumi)
        else:
            yield (current_run, output)
            output = [lumi]
        current_run = run
    yield (current_run, output)

def collapse_ranges_in_list(xs):
    '''
    Generate a list of contiguous ranges in a list of numbers.
    Example:
    >>> list(collapse_ranges_in_list([1, 2, 3, 5, 8, 9, 10]))
    [[1, 3], [5, 5], [8, 10]]
    '''
    output = []
    for x in xs:
        if not output:
            # Starting new range
            output = [x, x]
        elif x == output[1]+1:
            output[1] = x
        else:
            yield output
            output = [x, x]
    yield output


def json_summary(run_lumi_set, indent=2):
    '''
    Compute a crab -report like json summary for a set of runs and lumis.
    Example:
    >>> run_lumis = [(100, 2), (100, 1), (150, 1), (150, 2), (150, 8)]
    >>> # Disable indentation
    >>> json_summary(run_lumis, None)
    '{"100": [[1, 2]], "150": [[1, 2], [8, 8]]}'
    '''
    run_lumis = sorted(run_lumi_set)
    output = {}
    if not run_lumis:
        return output
    for run, lumis_in_run in group_by_run(run_lumis):
        output[str(run)] = list(collapse_ranges_in_list(lumis_in_run))
    return json.dumps(output, sort_keys=True, indent=indent)

def main():
    if argparse is None:
        sys.stderr.write("Could not import argparse from RecoLuminosity.LumiDB\n"
                        "Make sure your environment is setup for CMSSW\n")
        return 100

    parser = argparse.ArgumentParser(
        description='Parse a set of CMSSW framework job reports'
        'and compute the lumi json summary and skim efficiency.'
    )

    parser.add_argument('job_reports', type=str, nargs='+',
                        help='Path (or wildcard) to .xml job reports')

    parser.add_argument('--json-out', type=str, default='', dest='json_out',
                        help='Write run-lumi summary to file.')

    parser.add_argument('--size-report', default=False, const=True,
                        action='store_const', dest='size_report',
                        help='Check the sizes of the input and output files.'
                        ' Requires local file system access.')

    parser.add_argument('--output-dir', default='',
                        dest='output_dir',
                        help='Location of output directory')

    parser.add_argument('--check-overlaps', default=True, const=False,
                        action='store_const', dest='overlaps_ok',
                        help='If true, ensure no lumi is in two files!')

    user_name = os.environ.get('FARMOUT_USER', os.environ['USER'])

    parser.add_argument(
        '--output-prepend',
        default='root://cmsxrootd.hep.wisc.edu///store/user/%s/' % (user_name),
        dest='output_dir_prepend', help='Base output directory')

    args = parser.parse_args()

    # Flatten the list of input files
    files = chain(*[glob.glob(file) for file in args.job_reports])

    try:
        result = parse_job_reports(files, verbose=True,
                                   overlaps_ok=args.overlaps_ok)
    except (RunLumiCorruptionError, JobOverlapError), e:
        sys.stderr.write(e)
        return 1

    result.update(dict(
        reports=len(result['job_reports']),
        outputs=len(result['output_files']),
        inputs=len(result['input_files']),
        lumis=len(result['input_run_lumis']),
        min_run=min(x[0] for x in result['input_run_lumis']),
        max_run=max(x[0] for x in result['input_run_lumis']),
        writesperfile=result['events_written']*1./max(len(result['output_files']), 1),
        skim_eff=result['events_written']*100.0/result['events_read'],
    ))
    report = [
        "Read %(reports)i job reports, corresponding to %(inputs)i "
            "input files and %(lumis)i lumisections.",
        "The run range is: %(min_run)i-%(max_run)i",
        "%(outputs)i output file were produced.",
        "%(events_read)i events were processed.",
        "%(events_written)i events were written. (%(writesperfile)0.0f events/file)",
        "Skim efficiency: %(skim_eff)0.4f%%",
    ]

    if args.json_out:
        json_file = open(args.json_out, 'w')
        json_file.write(json_summary(result['input_run_lumis'], indent=None))
        result["json_out"] = args.json_out
        report.append("Lumi summary written to: %(json_out)s")

    if not result['ok']:
        report.append("Some files we not parsed:")
        report.extend(result['bad_files'])

    sys.stdout.write("Job Report Summary:\n")
    sys.stdout.write("\n".join(report) % result + "\n")

    if args.size_report:
        sys.stdout.write("Generating size report\n")

        # If necessary, add full path
        if args.output_dir:
            prefix = args.output_dir_prepend + args.output_dir + '/'
            result['output_files'] = [prefix+x for x in result['output_files']]

        input_file_sizes, output_file_sizes = get_file_sizes(
            result['input_files'],
            result['output_files']
        )

        clean_input_files = [size for file, size in input_file_sizes
                             if size >= 0]
        clean_output_files = [size for file, size in output_file_sizes
                             if size >= 0]

        input_size = sum(clean_input_files)
        output_size = sum(clean_output_files)

        result.update(dict(
            input_file_sizes=len(input_file_sizes),
            output_file_sizes=output_file_sizes,
            clean_input_files=len(clean_input_files),
            clean_output_files=clean_output_files,
            input_size=input_size/1e9,
            input_evt_size=input_size/(1e3*result['events_read']),
            output_size=output_size/1e9,
            output_evt_size=output_size/(1e3*result['events_written']),
        ))

        report = [
            "Got sizes for %(clean_input_files)i/%(input_file_sizes)i input files",
            "Input file size: %(input_size)0.3f GB, %(input_evt_size)0.0f kB/evt",
            "Got sizes for %(clean_output_files)i/%(output_file_sizes)i output files",
            "Output file size: %0.3f GB, %0.0f kB/evt",
        ]

        sys.stdout.write("\n".join(report) % result + "\n")

if __name__ == "__main__":
    try:
        ret = main()
    except KeyboardInterrupt:
        ret = None
    sys.exit(ret)
