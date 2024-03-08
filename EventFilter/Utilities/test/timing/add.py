process.hltPreDiphotonMVATest = cms.EDFilter( "HLTPrescaler",
     offset = cms.uint32( 0 ),
     L1GtReadoutRecordTag = cms.InputTag( "hltGtStage2Digis" )
)

process.hltDiEG14p25EtEta2p55UnseededFilter = cms.EDFilter( "HLT1Photon",
     saveTags = cms.bool( True ),
     inputTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     triggerType = cms.int32( 92 ),
     MinE = cms.double( -1.0 ),
     MinPt = cms.double( 14.25 ),
     MinMass = cms.double( -1.0 ),
     MaxMass = cms.double( -1.0 ),
     MinEta = cms.double( -1.0 ),
     MaxEta = cms.double( 2.55 ),
     MinN = cms.int32( 2 )
)
 
process.HLTDiphotonMVATestProducer = cms.EDProducer("MVATestProducer",
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     inputTagR9 = cms.InputTag("hltEgammaR9IDUnseeded", "r95x5"),
     inputTagHoE = cms.InputTag("hltEgammaHoverEUnseeded"),
     inputTagSigmaiEtaiEta = cms.InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"),
     inputTagE2x2 = cms.InputTag("hltEgammaClusterShapeUnseeded", "e2x2"),
     inputTagIso = cms.InputTag("hltEgammaEcalPFClusterIsoUnseeded"),
     mvaFileXgbB = cms.FileInPath("EventFilter/Utilities/data/barrel_py3.bin"),
     mvaFileXgbE = cms.FileInPath("EventFilter/Utilities/data/endcap_py3.bin"),
     mvaNTreeLimitB = cms.uint32(1498),
     mvaNTreeLimitE = cms.uint32(1500),
     mvaThresholdEt = cms.double(14.25)
)
 
process.HLTDiphotonMVATestCombFilter = cms.EDFilter("MVATestCombFilter",
     saveTags = cms.bool( False ),
     minMass = cms.double(95),
     mvaMinBarrel = cms.double(0.3),
     mvaMinEndcap = cms.double(0.3),
     mvaMinBarrelTight = cms.double(0.4),
     mvaMinEndcapTight = cms.double(0.4),
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     mvaPhotonTag = cms.InputTag( "HLTDiphotonMVATestProducer" ),
)
 
process.HLTDiphotonMvaTestSequence = cms.Sequence( process.HLTDoFullUnpackingEgammaEcalSequence + process.HLTPFClusteringForEgamma + process.hltEgammaCandidates + process.hltEGL1SingleAndDoubleEGOrFilter + process.hltEG30L1SingleAndDoubleEGOrEtFilter + process.HLTPFClusteringForEgammaUnseeded + process.hltEgammaCandidatesUnseeded + process.hltDiEG14p25EtEta2p55UnseededFilter + process.hltEgammaR9IDUnseeded + process.HLTDoLocalHcalSequence + process.HLTFastJetForEgamma + process.hltEgammaHoverEUnseeded + process.hltEgammaClusterShapeUnseeded + process.hltEgammaEcalPFClusterIsoUnseeded )

process.HLT_Diphoton_MVATest = cms.Path( process.HLTBeginSequence + process.hltL1sSingleAndDoubleEGor + process.hltPreDiphotonMVATest + process.HLTDiphotonMvaTestSequence  + process.HLTDiphotonMVATestProducer + process.HLTDiphotonMVATestCombFilter + process.HLTEndSequence )

process.schedule.insert( process.schedule.index( process.HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95_v19 ) + 1, process.HLT_Diphoton_MVATest )

process.PrescaleService.prescaleTable.append(cms.PSet(
     pathName = cms.string( "HLT_Diphoton_MVATest" ),
     prescales = cms.vuint32( 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0 )
))

getattr(process.datasets, 'EGamma0').append('HLT_Diphoton_MVATest')
getattr(process.datasets, 'EGamma1').append('HLT_Diphoton_MVATest')
getattr(process.datasets, 'OnlineMonitor').append('HLT_Diphoton_MVATest')

process.hltDatasetEGamma.triggerConditions.append('HLT_Diphoton_MVATest')
process.hltDatasetOnlineMonitor.triggerConditions.append('HLT_Diphoton_MVATest')

#print(process.schedule)
#for p in process.PrescaleService.prescaleTable:
#  print(p)
#print(process.datasets)
#print(process.hltDatasetEGamma.triggerConditions)
#print(process.hltDatasetOnlineMonitor.triggerConditions)

