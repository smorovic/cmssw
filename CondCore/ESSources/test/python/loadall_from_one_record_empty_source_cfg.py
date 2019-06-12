from __future__ import print_function
import time

import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing
from Configuration.AlCa.autoCond import autoCond
import six

options = VarParsing.VarParsing()
options.register('processId',
                 0,
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "Process Id")
options.register('connectionString',
                 #'sqlite_file:cms_conditions.db', #default value
                 'frontier://FrontierProd/CMS_CONDITIONS', #default value
                 #'oracle://cms_orcon_prod/CMS_CONDITIONS',
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.string,
                 "CondDB Connection string")
options.register('snapshotTime',
                 '', #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.string,
                 "GlobalTag snapshot time")
options.register('refresh',
                 0, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "Refresh type: default no refresh")
options.register('runNumber',
                 115, #default value, int limit -3
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "Run number; default gives latest IOV")
options.register('eventsPerLumi',
                 2, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "number of events per lumi")
options.register('numberOfLumis',
                 20, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "number of lumisections per run")
options.register('numberOfRuns',
                 1, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "number of runs in the job")
options.register('messageLevel',
                 0, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "Message level; default to 0")
options.register('security',
                 '', #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.string,
                 "FroNTier connection security: activate it with 'sig'")

options.parseArguments()

process = cms.Process("TEST")

process.MessageLogger = cms.Service( "MessageLogger",
                                     destinations = cms.untracked.vstring( 'detailedInfo' ),
                                     detailedInfo = cms.untracked.PSet( threshold = cms.untracked.string( 'INFO' ) ),
                                     )

CondDBParameters = cms.PSet( authenticationPath = cms.untracked.string( '/build/gg/' ),
                             authenticationSystem = cms.untracked.int32( 0 ),
                             messageLevel = cms.untracked.int32( options.messageLevel ),
                             security = cms.untracked.string( options.security ),
                             )

refreshAlways, refreshOpenIOVs, refreshEachRun, reconnectEachRun = False, False, False, False
if options.refresh == 0:
    refreshAlways, refreshOpenIOVs, refreshEachRun, reconnectEachRun = False, False, False, False
elif options.refresh == 1:
    refreshAlways = True
    refreshOpenIOVs, refreshEachRun, reconnectEachRun = False, False, False
elif options.refresh == 2:
    refreshAlways = False
    refreshOpenIOVs = True
    refreshEachRun, reconnectEachRun = False, False
elif options.refresh == 3:
    refreshAlways, refreshOpenIOVs = False, False
    refreshEachRun = True
    reconnectEachRun = False
elif options.refresh == 4:
    refreshAlways, refreshOpenIOVs, refreshEachRun = False, False, False
    reconnectEachRun = True

process.GlobalTag = cms.ESSource( "PoolDBESSource",
                                  DBParameters = CondDBParameters,
                                  connect = cms.string( options.connectionString ),
                                  snapshotTime = cms.string( options.snapshotTime ),
                                  toGet = cms.VPSet(cms.PSet(
                                      record = cms.string('LumiTestPayloadRcd'),
                                      tag = cms.string('lumi_test_v00'),
                                      refreshTime = cms.uint64( 1 )
                                  )),
                                  RefreshAlways = cms.untracked.bool( refreshAlways ),
                                  RefreshOpenIOVs = cms.untracked.bool( refreshOpenIOVs ),
                                  RefreshEachRun = cms.untracked.bool( refreshEachRun ),
                                  ReconnectEachRun = cms.untracked.bool( reconnectEachRun ),
                                  DumpStat = cms.untracked.bool( True ),
                                  )


#TODO: add VarParsing support for adding custom conditions
#process.GlobalTag.toGet.append( cms.PSet( record = cms.string( "BeamSpotObjectsRcd" ),
#                                          tag = cms.string( "firstcollisions" ),
#                                          connect = cms.string( "frontier://FrontierProd/CMS_CONDITIONS" ),
#                                          snapshotTime = cms.string('2014-01-01 00:00:00.000'),
#                                          )
#                                )

process.source = cms.Source( "FileBasedEmptySource",
                             interval = cms.uint32( 5 ),
                             maxEvents = cms.uint32( 20000 ),
                             pathForLastLumiFile = cms.string('/afs/cern.ch/user/c/condbpro/public/last_time.txt'),
                             firstLuminosityBlock = cms.untracked.uint32(1),
                             firstRun = cms.untracked.uint32( options.runNumber ),
                             firstTime = cms.untracked.uint64( ( long( time.time() ) - 24 * 3600 ) << 32 ), #24 hours ago in nanoseconds
                             numberEventsInRun = cms.untracked.uint32( 1000 ), # options.numberOfLumis lumi sections per run
                             numberEventsInLuminosityBlock = cms.untracked.uint32( options.eventsPerLumi )
                             )

#process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32( options.eventsPerLumi *  options.numberOfLumis * options.numberOfRuns ) ) #options.numberOfRuns runs per job

process.prod = cms.EDAnalyzer("LumiTestReadAnalyzer",
                              processId = cms.untracked.uint32( options.processId ),
                              pathForLastLumiFile = cms.untracked.string("./last_time.txt"),
                              pathForAllLumiFile = cms.untracked.string("./all_time.txt" ),
                              pathForErrorFile = cms.untracked.string("/build/gg/CMSSW_10_5_0/src/lumi_read_errors")
)

#process.get = cms.EDAnalyzer( "EventSetupRecordDataGetter",
#                              toGet =  cms.VPSet(),
#                              verbose = cms.untracked.bool( True )
#                              )

#process.escontent = cms.EDAnalyzer( "PrintEventSetupContent",
#                                    compact = cms.untracked.bool( True ),
#                                    printProviders = cms.untracked.bool( True )
#                                    )

#process.esretrieval = cms.EDAnalyzer( "PrintEventSetupDataRetrieval",
#                                      printProviders = cms.untracked.bool( True )
#                                      )

process.p = cms.Path( process.prod )
#process.esout = cms.EndPath( process.escontent + process.esretrieval )
#if process.schedule_() is not None:
#    process.schedule_().append( process.esout )

for name, module in six.iteritems(process.es_sources_()):
    print("ESModules> provider:%s '%s'" % ( name, module.type_() ))
for name, module in six.iteritems(process.es_producers_()):
    print("ESModules> provider:%s '%s'" % ( name, module.type_() ))
