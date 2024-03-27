process.hltPreDiphotonMVA = cms.EDFilter( "HLTPrescaler",
     offset = cms.uint32( 0 ),
     L1GtReadoutRecordTag = cms.InputTag( "hltGtStage2Digis" )
)

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
 
process.PhotonXGBoostProducer = cms.EDProducer("PhotonXGBoostProducer",
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     inputTagR9 = cms.InputTag("hltEgammaR9IDUnseeded", "r95x5"),
     inputTagHoE = cms.InputTag("hltEgammaHoverEUnseeded"),
     inputTagSigmaiEtaiEta = cms.InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"),
     inputTagE2x2 = cms.InputTag("hltEgammaClusterShapeUnseeded", "e2x2"),
     inputTagIso = cms.InputTag("hltEgammaEcalPFClusterIsoUnseeded"),
     mvaFileXgbB = cms.FileInPath("RecoEgamma/PhotonIdentification/data/XGBoost/Photon_NTL_168_Barrel_v1.bin"), #copy by hand
     mvaFileXgbE = cms.FileInPath("RecoEgamma/PhotonIdentification/data/XGBoost/Photon_NTL_158_Endcap_v1.bin"), #copy by hand
     mvaNTreeLimitB = cms.uint32(168),
     mvaNTreeLimitE = cms.uint32(158),
     mvaThresholdEt = cms.double(14.25)
)
 
process.HLTEgammaDoubleXGBoostCombFilter = cms.EDFilter("HLTEgammaDoubleXGBoostCombFilter",
     saveTags = cms.bool( True ),
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     mvaPhotonTag = cms.InputTag( "PhotonXGBoostProducer" ),
     highMassCut = cms.double(95),
     leadCutHighMass1 = cms.vdouble(0.98,0.95),
     subCutHighMass1 = cms.vdouble(0.00,0.04),
     leadCutHighMass2 = cms.vdouble(0.85,0.85),
     subCutHighMass2 = cms.vdouble(0.04,0.08),
     leadCutHighMass3 = cms.vdouble(0.30,0.50),
     subCutHighMass3 = cms.vdouble(0.15,0.20),
     lowMassCut = cms.double(60),
     leadCutLowMass1 = cms.vdouble(0.98,0.90),
     subCutLowMass1 = cms.vdouble(0.04,0.05),
     leadCutLowMass2 = cms.vdouble(0.90,0.80),
     subCutLowMass2 = cms.vdouble(0.10,0.10),
     leadCutLowMass3 = cms.vdouble(0.60,0.60),
     subCutLowMass3 = cms.vdouble(0.30,0.30),
)

#test path!
 
process.hltPreDiphotonMVATest = cms.EDFilter( "HLTPrescaler",
     offset = cms.uint32( 0 ),
     L1GtReadoutRecordTag = cms.InputTag( "hltGtStage2Digis" )
)


process.HLTDiphotonMVATestProducer = cms.EDProducer("MVATestProducer",
     usePathStatus = cms.bool( True ),
     hltResults = cms.InputTag( "" ),
     l1tResults = cms.InputTag( "" ),
     l1tIgnoreMaskAndPrescale = cms.bool( False ),
     throw = cms.bool( True ),
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     inputTagR9 = cms.InputTag("hltEgammaR9IDUnseeded", "r95x5"),
     inputTagHoE = cms.InputTag("hltEgammaHoverEUnseeded"),
     inputTagSigmaiEtaiEta = cms.InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"),
     inputTagE2x2 = cms.InputTag("hltEgammaClusterShapeUnseeded", "e2x2"),
     inputTagIso = cms.InputTag("hltEgammaEcalPFClusterIsoUnseeded"),
     mvaFileXgbB = cms.FileInPath("RecoEgamma/PhotonIdentification/data/XGBoost/Photon_NTL_168_Barrel_v1.bin"), #copy by hand
     mvaFileXgbE = cms.FileInPath("RecoEgamma/PhotonIdentification/data/XGBoost/Photon_NTL_158_Endcap_v1.bin"), #copy by hand
     mvaNTreeLimitB = cms.uint32(168),
     mvaNTreeLimitE = cms.uint32(158),
     mvaThresholdEt = cms.double(14.25),
     triggerConditions = cms.vstring(
       'L1Seed',
       'L1Seed_And_Filter',
       'HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90_v20',
       'HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95_v20',
       'HLT_Diphoton_MVA'
     ),
     highMassCut = cms.double(95),
     leadCutHighMass1 = cms.vdouble(0.98,0.95),
     subCutHighMass1 = cms.vdouble(0.00,0.04),
     leadCutHighMass2 = cms.vdouble(0.85,0.85),
     subCutHighMass2 = cms.vdouble(0.04,0.08),
     leadCutHighMass3 = cms.vdouble(0.30,0.50),
     subCutHighMass3 = cms.vdouble(0.15,0.20),
     lowMassCut = cms.double(60),
     leadCutLowMass1 = cms.vdouble(0.98,0.90),
     subCutLowMass1 = cms.vdouble(0.04,0.05),
     leadCutLowMass2 = cms.vdouble(0.90,0.80),
     subCutLowMass2 = cms.vdouble(0.10,0.10),
     leadCutLowMass3 = cms.vdouble(0.60,0.60),
     subCutLowMass3 = cms.vdouble(0.30,0.30),
)

process.HLTDiphotonMVATestCombFilter = cms.EDFilter("MVATestCombFilter",
     saveTags = cms.bool( True ),
     candTag = cms.InputTag( "hltEgammaCandidatesUnseeded" ),
     mvaPhotonTag = cms.InputTag( "HLTDiphotonMVATestProducer" ),
     highMassCut = cms.double(95),
     leadCutHighMass1 = cms.vdouble(0.98,0.95),
     subCutHighMass1 = cms.vdouble(0.00,0.04),
     leadCutHighMass2 = cms.vdouble(0.85,0.85),
     subCutHighMass2 = cms.vdouble(0.04,0.08),
     leadCutHighMass3 = cms.vdouble(0.30,0.50),
     subCutHighMass3 = cms.vdouble(0.15,0.20),
     lowMassCut = cms.double(60),
     leadCutLowMass1 = cms.vdouble(0.98,0.90),
     subCutLowMass1 = cms.vdouble(0.04,0.05),
     leadCutLowMass2 = cms.vdouble(0.90,0.80),
     subCutLowMass2 = cms.vdouble(0.10,0.10),
     leadCutLowMass3 = cms.vdouble(0.60,0.60),
     subCutLowMass3 = cms.vdouble(0.30,0.30),
)

#measure L1 seeding pass / fail
process.L1Seed = cms.Path( process.HLTBeginSequence + process.hltL1sSingleAndDoubleEGor )
process.L1Seed_And_Filter = cms.Path(process.HLTBeginSequence + process.hltL1sSingleAndDoubleEGor + process.HLTDoFullUnpackingEgammaEcalSequence + process.HLTPFClusteringForEgamma + process.hltEgammaCandidates + process.hltEGL1SingleAndDoubleEGOrFilter + process.hltEG30L1SingleAndDoubleEGOrEtFilter)


#MVA production path
process.HLTDiphotonMvaSequence = cms.Sequence( process.HLTDoFullUnpackingEgammaEcalSequence + process.HLTPFClusteringForEgamma + process.hltEgammaCandidates + process.hltEGL1SingleAndDoubleEGOrFilter + process.hltEG30L1SingleAndDoubleEGOrEtFilter + process.HLTPFClusteringForEgammaUnseeded + process.hltEgammaCandidatesUnseeded + process.hltDiEG14p25EtEta2p55UnseededFilter + process.hltEgammaR9IDUnseeded + process.HLTDoLocalHcalSequence + process.HLTFastJetForEgamma + process.hltEgammaHoverEUnseeded + process.hltEgammaClusterShapeUnseeded + process.hltEgammaEcalPFClusterIsoUnseeded )

process.HLT_Diphoton_MVA = cms.Path( process.HLTBeginSequence + process.hltL1sSingleAndDoubleEGor + process.hltPreDiphotonMVA + process.HLTDiphotonMvaSequence  + process.PhotonXGBoostProducer + process.HLTEgammaDoubleXGBoostCombFilter + process.HLTEndSequence )

#MVA path test sequence (no filters, unseeded)
process.HLTDiphotonMvaTestSequence = cms.Sequence( process.HLTDoFullUnpackingEgammaEcalSequence + process.HLTPFClusteringForEgamma  + process.hltEgammaCandidates + process.HLTPFClusteringForEgammaUnseeded + process.hltEgammaCandidatesUnseeded + process.hltEgammaR9IDUnseeded + process.HLTDoLocalHcalSequence + process.HLTFastJetForEgamma + process.hltEgammaHoverEUnseeded + process.hltEgammaClusterShapeUnseeded + process.hltEgammaEcalPFClusterIsoUnseeded )

#MVA test path (only prescale filter)
process.HLT_Diphoton_MVATest = cms.Path( process.HLTBeginSequence + process.hltPreDiphotonMVATest + process.HLTDiphotonMvaTestSequence  + process.HLTDiphotonMVATestProducer + process.HLTDiphotonMVATestCombFilter + process.HLTEndSequence )

#append to datasets
#process.Dataset_EGamma0 = cms.Path( process.HLTDatasetPathBeginSequence + process.hltDatasetEGamma + process.hltPreDatasetEGamma0 )
#process.Dataset_EGamma1 = cms.Path( process.HLTDatasetPathBeginSequence + process.hltDatasetEGamma + process.hltPreDatasetEGamma1 )
#process.PhysicsEGamma0Output = cms.FinalPath( process.hltOutputPhysicsEGamma0 )
#process.PhysicsEGamma1Output = cms.FinalPath( process.hltOutputPhysicsEGamma1 )


#full schedule
process.schedule = cms.Schedule( *(process.L1Seed, process.L1Seed_And_Filter, process.HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass90_v20, process.HLT_Diphoton30_22_R9Id_OR_IsoCaloId_AND_HE_R9Id_Mass95_v20, process.HLT_Diphoton_MVA, process.HLT_Diphoton_MVATest))


process.PrescaleService.prescaleTable.append(cms.PSet(
     pathName = cms.string( "HLT_Diphoton_MVA" ),
     prescales = cms.vuint32( 0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0 )
))


process.PrescaleService.prescaleTable.append(cms.PSet(
     pathName = cms.string( "HLT_Diphoton_MVATest" ),
     prescales = cms.vuint32( 0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0 )
))

process.datasets = cms.PSet()
#getattr(process.datasets, 'EGamma0').append('HLT_Diphoton_MVATest')
#getattr(process.datasets, 'EGamma1').append('HLT_Diphoton_MVATest')
#getattr(process.datasets, 'OnlineMonitor').append('HLT_Diphoton_MVATest')
#
#process.hltDatasetEGamma.triggerConditions.append('HLT_Diphoton_MVATest')
#process.hltDatasetOnlineMonitor.triggerConditions.append('HLT_Diphoton_MVATest')

#print(process.schedule)
#for p in process.PrescaleService.prescaleTable:
#  print(p)
#print(process.datasets)
#print(process.hltDatasetEGamma.triggerConditions)
#print(process.hltDatasetOnlineMonitor.triggerConditions)

def customizePrescaleSeeds(process):
    # Updated seed replacements dictionary with the new mappings
    seed_replacements = {
        'L1_DoubleMu0_Upt6_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
        'L1_DoubleMu0_Upt7_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
        'L1_DoubleMu0_Upt8_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
    }

    replaced_seeds = 0

    for moduleName in dir(process):
        module = getattr(process, moduleName)
        if hasattr(module, 'L1SeedsLogicalExpression'):
            l1SeedObj = module.L1SeedsLogicalExpression
            l1Seed = l1SeedObj.value()  # Extract the string value

            # Check and replace each possible seed
            for old_seed, new_seed in seed_replacements.items():
                if old_seed in l1Seed:
                    # Perform the replacement
                    l1Seed = l1Seed.replace(old_seed, new_seed)
                    module.L1SeedsLogicalExpression = cms.string(l1Seed)
                    replaced_seeds += 1
                    # print(f"Replaced {old_seed} with {new_seed} in {moduleName}")

    return process

process = customizePrescaleSeeds(process)
process.hltGtStage2ObjectMap.AXOL1TLModelVersion = cms.string("GTADModel_v3")

process.PrescaleService.forceDefault = cms.bool( True )

process.options.numberOfThreads=cms.untracked.uint32(1)
process.options.numberOfStreams=cms.untracked.uint32(1)
