import socket
import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing
process = cms.Process("LumiTestPopulator")
from CondCore.CondDB.CondDB_cfi import *

options = VarParsing.VarParsing()
options.register( 'destinationConnection'
                #, 'sqlite_file:cms_conditions.db' #default value
                , 'oracle://cms_orcon_prod/CMS_CONDITIONS' #default value
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.string
                , "Connection string to the DB where payloads will be possibly written."
                  )
options.register( 'tag'
                , 'lumi_test_v00'
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.string
                , "Tag written in destinationConnection."
                  )
options.register( 'runNumber'
                , 1 #default value
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.int
                , "Run number to be uploaded."
                  )
options.register( 'logFile'
                , 'lumi_pop.log'
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.string
                , "Log file name."
                  )
options.register( 'dataSize'
                , 1000
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.int
                , "Input data size; default to 1kb"
                  )
options.register( 'messageLevel'
                , 0 #default value
                , VarParsing.VarParsing.multiplicity.singleton
                , VarParsing.VarParsing.varType.int
                , "Message level; default to 0"
                  )
options.parseArguments()

CondDBConnection = CondDB.clone( connect = cms.string( options.destinationConnection ) )
CondDBConnection.DBParameters.messageLevel = cms.untracked.int32( options.messageLevel )
CondDBConnection.DBParameters.authenticationPath = cms.untracked.string( '/data/O2O' )

process.MessageLogger = cms.Service("MessageLogger",
                                    cout = cms.untracked.PSet(threshold = cms.untracked.string('INFO')),
                                    destinations = cms.untracked.vstring('cout')
                                    )

process.source = cms.Source("EmptyIOVSource",
                            timetype = cms.string('runnumber'),
                            firstRunnumber = cms.uint32( 1 ),
                            lastRunnumber  = cms.uint32( 1 ),
                            firstLumi = cms.uint32( 1 ),
                            lastLumi = cms.uint32( 1 ),
                            interval = cms.uint32(1),
                            maxLumiInRun = cms.uint32(100),
                            startTime = cms.string(""),
                            endTime = cms.string(""),
                            maxEvents = cms.uint32( 1000 )
                            )

process.PoolDBOutputService = cms.Service("PoolDBOutputService",
                                          CondDBConnection,
                                          timetype = cms.untracked.string('lumiid'),
                                          toPut = cms.VPSet(cms.PSet(record = cms.string('LumiTestRcd'),
                                                                     tag = cms.string( options.tag )
                                                                     )
                                                            )
                                          )

process.Test1 = cms.EDAnalyzer("LumiTestPopConAnalyzer",
                               SinceAppendMode = cms.bool(True),
                               record = cms.string('LumiTestRcd'),
                               name = cms.untracked.string('LumiTest'),
                               latency = cms.untracked.uint32( 2 ),
                               preLoadConnectionString = cms.untracked.string('frontier://FrontierProd/CMS_CONDITIONS'),
                               runNumber = cms.untracked.uint32( options.runNumber ),
                               #lastLumiUrl = cms.untracked.string( 'http://ru-c2e14-11-01.cms:11100/urn:xdaq-application:lid=52/getLatestLumiSection' ),
                               #preLoadConnectionString = cms.untracked.string('oracle://cms_orcoff_prep/CMS_CONDITIONS'),
                               #preLoadConnectionString = cms.untracked.string('sqlite_file:cms_conditions.db'),
                               pathForLastLumiFile = cms.untracked.string('/afs/cern.ch/user/c/condbpro/public/last_time.txt'),
                               Source = cms.PSet(
                                   maxDataSize = cms.untracked.uint32( options.dataSize ),
                                   debug=cms.untracked.bool(False)
                                                 ),
                               loggingOn = cms.untracked.bool(True),
                               IsDestDbCheckedInQueryLog = cms.untracked.bool(False)
                               )

process.p = cms.Path(process.Test1)
