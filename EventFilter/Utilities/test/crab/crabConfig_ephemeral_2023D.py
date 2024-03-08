from CRABClient.UserUtilities import config
config = config()

config.General.requestName = 'photonmva_ephemeral_2023D_v6'

config.JobType.pluginName = 'Analysis'
# Name of the CMSSW configuration file
config.JobType.psetName = 'hltData.py'

#config.Data.inputDataset = '/EphemeralHLTPhysics0/Run2023C-v1/RAW'
config.Data.inputDataset = '/EphemeralHLTPhysics0/Run2023D-v1/RAW'
config.Data.splitting = 'LumiBased'
#config.Data.splitting = 'Automatic'
config.Data.unitsPerJob = 30
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
