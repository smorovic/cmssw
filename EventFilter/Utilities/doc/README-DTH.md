
# DTH orbit/event unpacker for DAQSource

https://github.com/smorovic/cmssw/tree/15_0_0_pre1-source-improvements
This patch that unpacks the DTH data format into FedRawDataCollection.

It is rebased over CMSSW master (compatible with 15_0_0_pre1 at the time this file is commited), but it builds and runs in 14_2_0 as well. All changes is contained in EventFilter/Utilities:

# Fetching the code

Currently this code is found in private branch and will be commited to CMSSW master following more testing
```
scram project CMSSW_15_0_0_pre1 #or CMSSW_14_2_0 (currently it compiles and runs also in 14_X releases)
git cms-addpkg EventFilter/Utilities
git remote add smorovic https://github.com/smorovic/cmssw.git
git fetch smorovic 15_0_0_pre1-source-improvements:15_0_0_pre1-source-improvements
git checkout 15_0_0_pre1-source-improvements
scram b
```
 
Rerun the EventFilter/Utilities unit test (includes test for generated DTH payload):
```
cd EventFilter/Utilities/test
cmsenv
./RunBUFU.sh
```

#Important code and scripts in `EventFilter/Utilities`:

#definition of DTH orbit header, fragment trailer and SLinkRocket header/trailer (could potentially be moved to DataFormats or another package in the future):
<br>
[interface/DTHHeaders.h](../interface/DTHHeaders.h)

#plugin for DAQSource (input source) which parses the DTH format:
<br>
[src/DAQSourceModelsDTH.cc](../src/DAQSourceModelsDTH.cc)

#generator of dummy DTH payload for the fake "BU" process used in unit tests:
<br>
[plugins/DTHFakeReader.cc](../plugins/DTHFakeReader.cc)        

#script which runs the unit test with "fakeBU" process generating payload from multiple DTH sources (per orbit) and "FU" CMSSW job consuming it:
<br>
[test/testDTH.sh](../test/testDTH.sh)

<br>
FU cmsRun configuration used in above tests:
[test/unittest_FU_daqsource.py](../test/unittest_FU_daqsource.py)

# Running on custom files
`unittest_FU_daqsource.py` script can be used as a starting point to create a custom runner with inputs such as DTH dumps (not generated as in the unit test). DAQSource should be set to `dataMode = cms.untracked.string("DTH")` is set to process DTH format. Change "fileListMode" to True and fill in "fileList" with file paths to run with custom files, however they should be named similarly (and potentially also be placed in similar directory structure `ramdisk/runXX`, to provide initial run and lumisection to the source. Run number is also passed to the source via the command line (see also `testDTH.sh` script).


Note on the format of files that can be processed by the DTH module: apart of parsing single DTH orbit dump, input source plugin is capable also of building events from multiple DTH orbit blocks, but for the same orbit they must come sequentially in the file . Source scans the file and will find all blocks with orbit headers from the same orbit number (until a different orbit number is found or EOF), then it proceeds to build events from them by starting from last DTH event fragment trailer in each of the orbits found. This is then repeated for the next set of orbit blocks with the same orbit number in the file until file is processed.

In the future we might add another DAQ-specific header format to better encapsulate orbits (and we might also add file header similar to Run2/3 RAW data) with additional metadata to improve integrity and completeness checks.
 
