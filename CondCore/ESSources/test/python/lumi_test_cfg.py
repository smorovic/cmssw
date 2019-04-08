import FWCore.ParameterSet.Config as cms

process = cms.Process("TEST")
process.load("CondCore.DBCommon.CondDBCommon_cfi")
process.CondDBCommon.connect = cms.string("sqlite_file:lumitest.db")
process.CondDBCommon.DBParameters.messageLevel = 3

process.PoolDBESSource = cms.ESSource("PoolDBESSource",
    process.CondDBCommon,
    toGet = cms.VPSet(cms.PSet(
        record = cms.string('LumiTestPayloadRcd'),
        tag = cms.string('lumi_test_v00')
    ))
)

process.source = cms.Source("EmptyIOVSource",
                            timetype = cms.string('lumiid'),
                            firstRunnumber = cms.uint32( 100 ),
                            lastRunnumber  = cms.uint32( 102 ),
                            firstLumi = cms.uint32( 1 ),
                            lastLumi = cms.uint32( 20 ),
                            interval = cms.uint32(1),
                            maxLumiInRun = cms.uint32(100),
                            startTime = cms.string(""),
                            endTime = cms.string(""),
                            maxEvents = cms.uint32( 1000 )
)

process.prod = cms.EDAnalyzer("LumiTestReadAnalyzer")

process.p = cms.Path(process.prod)


