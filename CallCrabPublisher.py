import Publisher
import CrabLogger
import WorkSpace
import os
import sys
import getopt

long_options = [
	"dbs-url=",
	"ProcessedDataset=",
	"pset="
]

# config params for CRAB
cfg_params = {}
cfg_params["USER.copy_data"] = 1
cfg_params["USER.publish_data"] = 1
cfg_params["CMSSW.datasetpath"] = "NONE"
cfg_params["CMSSW.dataset_pu"] = None

options,args = getopt.getopt(sys.argv[1:],"",long_options)

for option,value in options:
	if option == "--dbs-url":
		cfg_params["USER.dbs_url_for_publication"] = value
	elif option == "--ProcessedDataset":
		cfg_params["USER.publish_data_name"] = value
	elif option == "--pset":
		cfg_params["CMSSW.pset"] = value


# initialize some global stuff the CRAB publisher depends on
Publisher.common.work_space = WorkSpace.WorkSpace(os.getcwd(),cfg_params)
Publisher.common.debugLevel = 0
Publisher.common.logger = CrabLogger.CrabLogger(args)
Publisher.common.logger.debug_level = 6

p = Publisher.Publisher(cfg_params)
p.run()
