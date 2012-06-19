#!/usr/bin/env python

'''

Clean CRAB-server created duplicates from a list of files streamed to stdin.

Author: Evan K. Friis, UW

The glidein server sometimes submits multiple successful jobs, so you end up
with:

    output_98_0_iLT.root
    output_98_1_iLT.root
    output_98_1_tjj.root

The first is an initial failed job that was resubmitted.  The second are
multiple results from the resubmission which are identical. This resolves these
duplicates by taking the newest if they are the same size, and taking the
largest if they differ (with a warning on stderr.)

'''

import sys
import os
from optparse import OptionParser


def main(input_files, verbose=False):
    '''
    Filter a list to remove duplicates
    '''
    output_files = []

    # Read in all files from input
    files = [file.strip() for file in input_files]
    # Now map the files by CRAB job ID
    # The key is the crab job index (i.e. 98 in the docs string)
    crab_outputs = {}
    for file in files:
        jobid = int(file.strip().split('_')[-3])
        crab_outputs.setdefault(jobid, []).append(file.strip())

    # Now go through and resolve any conflicts
    for job in sorted(crab_outputs.keys()):
        job_results = crab_outputs[job]
        # Sort the overlapping results by desirability
        # Order:
        # 1) submission index
        # 2) file size
        # 3) filename
        # Bigger is better.

        def key_func(filename):
            subindex = int(filename.split('_')[-2])
            size = os.path.getsize(filename)
            return (subindex, size, filename)

        job_results.sort(key=key_func, reverse=True)
        output_files.append(job_results[0])
        if verbose and len(job_results) > 1:
            for skipped in job_results[1:]:
                sys.stderr.write("Skipping %s\n" % skipped)
    return output_files

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-v", "--verbose", dest="verbose",
                      action='store_true', default=False,
                      help="Print skipped files to stderr")
    (options, args) = parser.parse_args()

    inputs = (x for x in sys.stdin.readlines())
    outputs = main(inputs, options.verbose)

    for output in outputs:
        sys.stdout.write(output + '\n')

