#!/usr/bin/env python

'''
dbsMaskFiles.py

Author: Evan K. Friis, UW Madison

Retrieve a list of logical file names from a DBS dataset that have valid lumi
blocks in them.  The luminosity mask is defined by a JSON file.

'''

import sys
import logging
import tempfile

try:
    import DataDiscovery
except ImportError:
    DataDiscovery = None

try:
    import RecoLuminosity.LumiDB.argparse as argparse
except ImportError:
    argparse = None

def main():
    if DataDiscovery is None:
        sys.stderr.write("Could not import CRAB DataDiscovery tool!\n"
                "Make sure your environment is setup for crab.\n"
                "https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab\n")
    elif argparse is None:
        sys.stderr.write("Could not import argparse from RecoLuminosity.LumiDB\n"
                    "Make sure your environment is setup for CMSSW\n")
    if not all((DataDiscovery, argparse)):
        return 100
        
    # Set up some global nonsense that crab uses
    import common
    common.logger = logging

    class Workspace:
        def __init__(self):
            self.fakeShareDir = tempfile.mkdtemp()
        def shareDir(self):
            return self.fakeShareDir

    common.work_space = Workspace()

    parser = argparse.ArgumentParser(
        description='Determine the relevant subset of files from a DBS dataset'
        ' using the given JSON lumi mask.\n\n'
    )

    parser.add_argument('dataset', type=str,
                        help='Full path of DBS dataset')

    parser.add_argument('json', type=str,
                        help='Path to JSON file mask')

    parser.add_argument('--run-range', default='', dest='runrange', type=str,
                        help='Use a given runrange.  Format: START-END')

    parser.add_argument('-v', default=False, const=True, action='store_const',
                        help='Log information to stderr')

    args = parser.parse_args()

    cfg = {
        'CMSSW.lumi_mask' : args.json,
    }

    if args.runrange:
        cfg['CMSSW.runselection'] = args.runrange

    if args.v:
        sys.stderr.write('Masking dataset: %s\n' % args.dataset)
        sys.stderr.write('with json: %s\n' % args.json)

    # Build the data discovery service
    discovery = DataDiscovery.DataDiscovery(args.dataset, cfg, None)
    discovery.fetchDBSInfo()

    # Get a dictionary with key = file, value = [valid lumis in file]
    file_to_lumi_map = discovery.getLumis()

    n_files = len(file_to_lumi_map)
    n_masked = 0
    # We only care about files with valid lumis in them
    for file, lumi in file_to_lumi_map.iteritems():
        if lumi:
            n_masked += 1
            sys.stdout.write(file + '\n')

    if args.v:
        sys.stderr.write(
            'After masking (%i/%i) files remain.\n' % (n_masked, n_files))

if __name__ == "__main__":
    try:
        ret = main()
    except KeyboardInterrupt:
        ret = None
    sys.exit(ret)
