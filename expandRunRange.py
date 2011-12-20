#!/bin/env python

'''

Expand an input list of run ranges.

Example:

    >> expandRunRange.py 1,2,3-7,9
    >> 1,2,3,4,5,6,7,9

Run unit test with: python -m doctest expandRunRange.py

Author: Evan K. Friis, UW Madison

'''

import sys
import re

def main():
    # Get all command line arguments
    arguments = sys.argv[1:]
    try:
        output_runs = expandRunRange(arguments)
    except (TypeError, ValueError), e:
        sys.stderr.write("%s: %s\n" % (sys.argv[0], e.message))
        return 1
    sys.stdout.write(','.join('%i' % x for x in output_runs) + '\n')

range_matcher = re.compile('(?P<start>\d+)-(?P<end>\d+)')

def expandRunRange(runrange):
    '''
    Concatenate a range of arguments
    >>> main(['1,2,3,4,5-7'])
    1,2,3,4,5,6,7

    There can also be spaces.

    >>> main(['1,2,3', '4,5-7'])
    1,2,3,4,5,6,7

    '''

    # Turn into a single comma separated list
    joined_arguments = ','.join(runrange)
    output_runs = []
    # Look at each cleaned argument
    for arg in joined_arguments.split(','):
        # Skip blank arguments
        if not arg:
            continue
        # Check if it's a range
        match = range_matcher.match(arg)
        if match:
            start = int(match.group('start'))
            end = int(match.group('end'))
            if end < start:
                raise TypeError("end of range %s is less than the start" % arg)
            output_runs.extend(range(start, end+1))
        else:
            try:
                output_runs.append(int(arg))
            except ValueError, e:
                e.message = "can't parse argument %r" % arg
                raise

    return output_runs

if __name__ == "__main__":
    try:
        ret = main()
    except KeyboardInterrupt:
        ret = None
    sys.exit(ret)
