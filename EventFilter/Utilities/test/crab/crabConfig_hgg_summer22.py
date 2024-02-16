from CRABClient.UserUtilities import config
config = config()

config.General.requestName = 'mva_hgg_s2022_v4'

config.JobType.pluginName = 'Analysis'
# Name of the CMSSW configuration file
config.JobType.psetName = 'hltMC_2022.py'

#config.Data.inputDataset = '/EphemeralHLTPhysics0/Run2023C-v1/RAW'
config.Data.inputDataset = '/GluGluHToGG_M-125_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer22DRPremix-BSzpz35_124X_mcRun3_2022_realistic_v12-v3/GEN-SIM-RAW'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 10
config.Data.publication = False
# This string is used to construct the output dataset name
#config.Data.outputDatasetTag = 'CRAB3_Analysis_test1'

# These values only make sense for processing data
#    Select input data based on a lumi mask
#
## config.Data.lumiMask = '/eos/user/c/cmsdqm/www/CAF/certification/Run3/PromptReco/Run3_2022_2023_Golden.json'
#    Select input data based on run-ranges

## config.Data.runRange = '367260'

config.JobType.outputFiles = ['photon_mva.root']

# Where the output files will be transmitted to
config.Site.storageSite = 'T2_CH_CERN'
