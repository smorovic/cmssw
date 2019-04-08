#include "CondCore/PopCon/interface/PopConAnalyzer.h"
#include "CondCore/PopCon/interface/PopConForLumiToHLT.h"
#include "CondTools/RunInfo/interface/LumiTestPopConSourceHandler.h"
#include "FWCore/Framework/interface/MakerMacros.h"



typedef popcon::PopConGenericAnalyzer<LumiTestPopConSourceHandler,popcon::PopConForLumiToHLT> LumiTestPopConAnalyzer;
//define this as a plug-in
DEFINE_FWK_MODULE(LumiTestPopConAnalyzer);
