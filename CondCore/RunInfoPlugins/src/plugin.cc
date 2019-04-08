#include "CondCore/ESSources/interface/registration_macros.h"
#include "CondFormats/DataRecord/interface/RunSummaryRcd.h"
#include "CondFormats/RunInfo/interface/RunSummary.h"
#include "CondFormats/RunInfo/interface/RunInfo.h"
#include "CondFormats/DataRecord/interface/L1TriggerScalerRcd.h"
#include "CondFormats/RunInfo/interface/L1TriggerScaler.h"
#include "CondFormats/DataRecord/interface/MixingRcd.h"
#include "CondFormats/RunInfo/interface/MixingModuleConfig.h"
#include "CondFormats/DataRecord/interface/FillInfoRcd.h"
#include "CondFormats/RunInfo/interface/FillInfo.h"
#include "CondFormats/DataRecord/interface/LHCInfoRcd.h"
#include "CondFormats/RunInfo/interface/LHCInfo.h"
#include "CondFormats/DataRecord/interface/LumiTestPayloadRcd.h"
#include "CondFormats/Common/interface/LumiTestPayload.h"

REGISTER_PLUGIN(RunSummaryRcd,RunSummary);
REGISTER_PLUGIN(RunInfoRcd,RunInfo);
REGISTER_PLUGIN(L1TriggerScalerRcd, L1TriggerScaler);
REGISTER_PLUGIN(MixingRcd,MixingModuleConfig);
REGISTER_PLUGIN(FillInfoRcd,FillInfo);
REGISTER_PLUGIN(LHCInfoRcd,LHCInfo);
REGISTER_PLUGIN(LumiTestPayloadRcd,cond::LumiTestPayload);
