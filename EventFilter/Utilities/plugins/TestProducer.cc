#include <memory>

#include "TestProducer.h"

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "DataFormats/RecoCandidate/interface/RecoEcalCandidate.h"
#include "DataFormats/EgammaReco/interface/SuperCluster.h"
#include "DataFormats/EgammaReco/interface/SuperClusterFwd.h"
#include "DataFormats/Common/interface/AssociationMap.h"
//#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"

#include "FWCore/MessageLogger/interface/MessageLogger.h"

MVATestProducer::MVATestProducer(edm::ParameterSet const& config) :
//      //if getting cands from a filter
//      candToken_(consumes<trigger::TriggerFilterObjectWithRefs>(config.getParameter<edm::InputTag>("candTag"))),
//      //if we consume a producer
      candToken_(consumes<reco::RecoEcalCandidateCollection>(config.getParameter<edm::InputTag>("candTag"))),
      tokenR9_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagR9"))),
      tokenHoE_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagHoE"))),
      tokenSigmaiEtaiEta_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagSigmaiEtaiEta"))),
      tokenE2x2_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagE2x2"))),
      tokenIso_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagIso"))),
      mvaFileB_(config.getParameter<edm::FileInPath>("mvaFileB")),
      mvaFileE_(config.getParameter<edm::FileInPath>("mvaFileE"))
  {
    mvaEstimatorB_ = std::make_unique<photonMvaEstimator>(mvaFileB_);
    mvaEstimatorE_ = std::make_unique<photonMvaEstimator>(mvaFileE_);
    //produces<std::vector<float>>();//TODO
    produces<reco::RecoEcalCandidateIsolationMap>();
  }

  void MVATestProducer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.add<edm::InputTag>("candTag");
    desc.add<edm::InputTag>("inputTagR9", edm::InputTag("hltEgammaR9IDUnseeded", "r95x5"));
    desc.add<edm::InputTag>("inputTagHoE", edm::InputTag("hltEgammaHoverEUnseeded"));
    desc.add<edm::InputTag>("inputTagSigmaiEtaiEta", edm::InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"));
    desc.add<edm::InputTag>("inputTagE2x2", edm::InputTag("hltEgammaClusterShapeUnseeded", "e2x2"));
    desc.add<edm::InputTag>("inputTagIso", edm::InputTag("hltEgammaEcalPFClusterIsoUnseeded"));
    desc.add<edm::FileInPath>("mvaFileB",
                              edm::FileInPath("/afs/cern.ch/work/r/rlee/public/CMSSW_13_3_0/src/xgbModels/M7L25_GGH13andDataD_NoTrkIso_M60_PdgIDCut_1213_Barrel.xml"));
    desc.add<edm::FileInPath>("mvaFileE",
                              edm::FileInPath("/afs/cern.ch/work/r/rlee/public/CMSSW_13_3_0/src/xgbModels/M7L25_GGH13andDataD_NoTrkIso_M60_PdgIDCut_1213_Endcap.xml"));
  }

  void MVATestProducer::produce(edm::Event& event, edm::EventSetup const& setup) {

//    //ifdef we get cands from a filter
//    // Ref to Candidate object to be recorded in filter object
//    edm::Ref<reco::RecoEcalCandidateCollection> ref;
//
//    edm::Handle<trigger::TriggerFilterObjectWithRefs> PrevFilterOutput;
//    event.getByToken(candToken_, PrevFilterOutput);
//
//    std::vector<edm::Ref<reco::RecoEcalCandidateCollection> > recoecalcands;
//
//    PrevFilterOutput->getObjects(trigger::TriggerCluster, recoecalcands);
//    if (recoecalcands.empty())
//      PrevFilterOutput->getObjects(trigger::TriggerPhoton, recoecalcands);


    edm::Handle<reco::RecoEcalCandidateCollection> recCollection; // hltEgammaCandidates(Unseeded)
    event.getByToken(candToken_, recCollection);

    //get hold of r9 association map
    edm::Handle<reco::RecoEcalCandidateIsolationMap> r9Map;
    event.getByToken(tokenR9_, r9Map);

    //get hold of HoE association map
    edm::Handle<reco::RecoEcalCandidateIsolationMap> hoEMap;
    event.getByToken(tokenHoE_, hoEMap);

    //get hold of isolated association map
    edm::Handle<reco::RecoEcalCandidateIsolationMap> sigmaiEtaiEtaMap;
    event.getByToken(tokenSigmaiEtaiEta_, sigmaiEtaiEtaMap);

    //get hold of isolated association map
    edm::Handle<reco::RecoEcalCandidateIsolationMap> e2x2Map;
    event.getByToken(tokenE2x2_, e2x2Map);

    //get hold of isolated association map
    edm::Handle<reco::RecoEcalCandidateIsolationMap> isoMap;
    event.getByToken(tokenIso_, isoMap);

    //output
    reco::RecoEcalCandidateIsolationMap mvaScoreMap(recCollection);
//    //if taking trigger cands
//    for (unsigned int i = 0; i < recoecalcands.size(); i++) {
//      ref = recoecalcands[i];
//      //edm::Ref<reco::RecoEcalCandidateCollection> ref(recoecalcands, i);
//
    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

      float EtaSC = ref->eta();
      float PhiSC = ref->phi();

      float r9 = (*r9Map).find(ref)->val;
      float hoe = (*hoEMap).find(ref)->val;
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
      float e2x2 = (*e2x2Map).find(ref)->val;
      float iso = (*isoMap).find(ref)->val;

      float rawE = ref->superCluster()->rawEnergy();
      float etaW = ref->superCluster()->etaWidth();
      float phiW = ref->superCluster()->phiWidth();

      float scEnergy = ref->superCluster()->energy();
      float scEt = scEnergy * sin(2 * atan(exp(-EtaSC)));
      if (scEnergy < 0.)
        scEnergy = 0.;
      if (scEt < 0.)
        scEt = 0.; /* first and second order terms assume non-negative energies */

      //TODO: do MVA calculation with cand pairs?
      float photonScore = 0;
      if (abs(EtaSC) < 1.5) photonScore = mvaEstimatorB_->computeMva(rawE,r9,siEtaiEta,etaW,phiW,e2x2,EtaSC,hoe,iso);
      if (abs(EtaSC) > 1.5) photonScore = mvaEstimatorE_->computeMva(rawE,r9,siEtaiEta,etaW,phiW,e2x2,EtaSC,hoe,iso);

      edm::LogWarning("DiphotonMVAMVATestProducer") << "PhotonScore:" << photonScore << " variables: EtaSC:" << EtaSC << " PhiSC:" << PhiSC << " R9:" << r9 << " HOE:" << hoe
                << " sihih:" << siEtaiEta << " iso:" << iso << " rawE:" << rawE << " etaW:" << etaW
                << " phiW:" << phiW << " scEt:" << scEt << " s4(e2x2):" << e2x2;

      mvaScoreMap.insert(ref, photonScore);

    }

    //std::unique_ptr<std::vector<float>> pi(new std::vector<float>());
    //event.put(std::move(pi));
    iEvent.put(std::make_unique<reco::RecoEcalCandidateIsolationMap>(mvaScoreMap));

  }

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestProducer);
