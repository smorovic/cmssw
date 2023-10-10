#!/bin/bash
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function diebu {  echo Failure $1: status $2 ; echo "" ; echo "----- Error -----"; echo ""; cat out_2_bu.log;  rm -rf $3/{ramdisk,data,dqmdisk,*.py}; exit $2 ; }
function diefu {  echo Failure $1: status $2 ; echo "" ; echo "----- Error -----"; echo ""; cat out_2_fu.log;  rm -rf $3/{ramdisk,data,dqmdisk,*.py}; exit $2 ; }
function diedqm { echo Failure $1: status $2 ; echo "" ; echo "----- Error -----"; echo ""; cat out_2_dqm.log; rm -rf $3/{ramdisk,data,dqmdisk,*.py}; exit $2 ; }

FUSCRIPT="unittest_FU_multi.py"
if [ ! -z $1 ]; then
  if [ "$1" == "local" ]; then
    FUSCRIPT="startFU.py"
    echo "local run: using ${FUSCRIPT}"
  fi
fi

if [ -z  ${SCRAM_TEST_PATH} ]; then
SCRAM_TEST_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
fi
echo "SCRAM_TEST_PATH = ${SCRAM_TEST_PATH}"

RC=0
P=$$
PREFIX=results_${USER}${P}
OUTDIR=${PWD}/${PREFIX}

echo "OUT_TMP_DIR = $OUTDIR"

mkdir ${OUTDIR}
cp ${SCRIPTDIR}/startBU.py ${OUTDIR}
cp ${SCRIPTDIR}/startFU.py ${OUTDIR}
cp ${SCRIPTDIR}/unittest_FU_multi.py ${OUTDIR}
cd ${OUTDIR}

rm -rf $OUTDIR/{ramdisk,data,dqmdisk,*.log}

runnumber="100101"

echo "Running test with FRD file header v2"
CMDLINE_STARTBU="cmsRun startBU.py runNumber=${runnumber} fffBaseDir=${OUTDIR} maxLS=10 fedMeanSize=128 eventsPerFile=20 eventsPerLS=20 frdFileVersion=2"
CMDLINE_STARTFU="cmsRun ${FUSCRIPT} runNumber=${runnumber} fffBaseDir=${OUTDIR}"
${CMDLINE_STARTBU}  > out_2_bu.log 2>&1 || diebu "${CMDLINE_STARTBU}" $? $OUTDIR
${CMDLINE_STARTFU}

rm -rf $OUTDIR/{ramdisk,data,*.log}

#no failures, clean up everything including logs if there are no errors
echo "Completed sucessfully"
#rm -rf $OUTDIR/{ramdisk,data,*.py,*.log}
rm -rf $OUTDIR

exit ${RC}
