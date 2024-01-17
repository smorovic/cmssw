#include "MVATestProducer.h"

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "DataFormats/RecoCandidate/interface/RecoEcalCandidate.h"
#include "DataFormats/EgammaReco/interface/SuperCluster.h"
#include "DataFormats/EgammaReco/interface/SuperClusterFwd.h"
#include "DataFormats/Common/interface/AssociationMap.h"

#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include <memory>


#include "TFile.h"
#include "TTree.h"

MVATestProducer::MVATestProducer(edm::ParameterSet const& config) :
      //if getting cands from a filter
      //candToken_(consumes<trigger::TriggerFilterObjectWithRefs>(config.getParameter<edm::InputTag>("candTag"))),

      //if consuming a producer
      candToken_(consumes<reco::RecoEcalCandidateCollection>(config.getParameter<edm::InputTag>("candTag"))),
      tokenR9_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagR9"))),
      tokenHoE_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagHoE"))),
      tokenSigmaiEtaiEta_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagSigmaiEtaiEta"))),
      tokenE2x2_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagE2x2"))),
      tokenIso_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagIso"))),
      mvaFileXgbB_(config.getParameter<edm::FileInPath>("mvaFileXgbB")),
      mvaFileXgbE_(config.getParameter<edm::FileInPath>("mvaFileXgbE")),
      mvaNTreeLimitB_(config.getParameter<unsigned int>("mvaNTreeLimitB")),
      mvaNTreeLimitE_(config.getParameter<unsigned int>("mvaNTreeLimitE")),
      mvaThresholdEt_(config.getParameter<unsigned int>("mvaThresholdEt"))
#ifdef DEBUG_EGAMMA_MVA
      ,rootFileName_(config.getUntrackedParameter<std::string>("treeFile", "photon_mva.root"))
#endif
{
    mvaEstimatorB_ = std::make_unique<PhotonMvaEstimator>(mvaFileXgbB_, mvaNTreeLimitB_);
    mvaEstimatorE_ = std::make_unique<PhotonMvaEstimator>(mvaFileXgbE_, mvaNTreeLimitE_);
    mvaEstimatorE_->computeMvaTest();
    produces<reco::RecoEcalCandidateIsolationMap>();

#ifdef DEBUG_EGAMMA_MVA

    et_ = new std::vector<float>();
    scEt_ = new std::vector<float>();
    phi_ = new std::vector<float>();
    r9_ = new std::vector<float>();
    siEtaiEta_ = new std::vector<float>();
    rawEnergy_ = new std::vector<float>();
    etaW_ = new std::vector<float>();
    phiW_ = new std::vector<float>();
    e2x2_ = new std::vector<float>();
    eta_ = new std::vector<float>();
    hoe_ = new std::vector<float>();
    iso_ = new std::vector<float>();
    mvaScoreXGB_ = new std::vector<float>();
    f_ = new TFile(rootFileName_.c_str(), "RECREATE");
    t_ = new TTree("HLTPhotonMVA", "HLT Photon MVA");
    t_->Branch("eventId", &eventId_, "eventId/l");
    t_->Branch("et", "std::vector<float>", &et_);
    t_->Branch("scEt", "std::vector<float>", &scEt_);
    t_->Branch("phi", "std::vector<float>", &phi_);
    t_->Branch("r9", "std::vector<float>", &r9_);
    t_->Branch("siEtaiEta", "std::vector<float>", &siEtaiEta_);
    t_->Branch("rawEnergy", "std::vector<float>", &rawEnergy_);
    t_->Branch("etaW", "std::vector<float>", &etaW_);
    t_->Branch("phiW", "std::vector<float>", &phiW_);
    t_->Branch("e2x2", "std::vector<float>", &e2x2_);
    t_->Branch("eta", "std::vector<float>", &eta_);
    t_->Branch("HoE", "std::vector<float>", &hoe_);
    t_->Branch("iso", "std::vector<float>", &iso_);
    t_->Branch("mvaScoreXGB", "std::vector<float>", &mvaScoreXGB_);

#endif

}

void MVATestProducer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.add<edm::InputTag>("candTag");
    desc.add<edm::InputTag>("inputTagR9", edm::InputTag("hltEgammaR9IDUnseeded", "r95x5"));
    desc.add<edm::InputTag>("inputTagHoE", edm::InputTag("hltEgammaHoverEUnseeded"));
    desc.add<edm::InputTag>("inputTagSigmaiEtaiEta", edm::InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"));
    desc.add<edm::InputTag>("inputTagE2x2", edm::InputTag("hltEgammaClusterShapeUnseeded", "e2x2"));
    desc.add<edm::InputTag>("inputTagIso", edm::InputTag("hltEgammaEcalPFClusterIsoUnseeded"));
    desc.add<edm::FileInPath>("mvaFileXgbB",
                              edm::FileInPath("EventFilter/Utilities/data/barrel.bin"));
    desc.add<edm::FileInPath>("mvaFileXgbE",
                              edm::FileInPath("EventFilter/Utilities/data/endcap.bin"));
    desc.add<unsigned int>("mvaNTreeLimitB", 55);
    desc.add<unsigned int>("mvaNTreeLimitE", 48);
    desc.add<unsigned int>("mvaThresholdEt", 0);
}

#ifdef DEBUG_EGAMMA_MVA //have ntuple
void MVATestProducer::produce(edm::Event& event, edm::EventSetup const& setup) {
#else
void MVATestProducer::produce(edm::StreamID, edm::Event& event, edm::EventSetup const& setup) const {
#endif

    //edm::Handle<trigger::TriggerFilterObjectWithRefs> PrevFilterOutput;
    //event.getByToken(candToken_, PrevFilterOutput);

    //std::vector<edm::Ref<reco::RecoEcalCandidateCollection> > filterCands;

    //PrevFilterOutput->getObjects(trigger::TriggerCluster, filterCands);
    //if (recCands.empty())
    //  PrevFilterOutput->getObjects(trigger::TriggerPhoton, filterCands);

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

#ifdef DEBUG_EGAMMA_MVA
      eventId_ = event.eventAuxiliary().event();
      et_->clear();
      scEt_->clear();
      phi_->clear();
      rawEnergy_->clear();
      r9_->clear();
      siEtaiEta_->clear();
      etaW_->clear();
      phiW_->clear();
      e2x2_->clear();
      eta_->clear();
      hoe_->clear();
      iso_->clear();
      mvaScoreXGB_->clear();
#endif

    //output
    reco::RecoEcalCandidateIsolationMap mvaScoreMap(recCollection);

    ////if taking trigger cands
    //for (unsigned int i = 0; i < recCollection.size(); i++) {
    //  edm::Ref<reco::RecoEcalCandidateCollection> ref = recCollection[i];
    //  //edm::Ref<reco::RecoEcalCandidateCollection> ref(recoecalcands, i);

    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

      float etaSC = ref->eta();

      float scEnergy = ref->superCluster()->energy();
      float r9 = (*r9Map).find(ref)->val;
      float hoe = (*hoEMap).find(ref)->val / scEnergy;
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
      float e2x2 = (*e2x2Map).find(ref)->val;
      float iso = (*isoMap).find(ref)->val;

      float rawEnergy = ref->superCluster()->rawEnergy();
      float etaW = ref->superCluster()->etaWidth();
      float phiW = ref->superCluster()->phiWidth();

      float scEt = scEnergy * sin(2 * atan(exp(-etaSC)));
      if (scEt < 0.)
        scEt = 0.; /* first and second order terms assume non-negative energies */

      float xgbScore = -100.;
      //compute only above threshold used for training and cand filter, else store negative value.
      if (scEt >= mvaThresholdEt_) {
        if (abs(etaSC) < 1.5)
          xgbScore = mvaEstimatorB_->computeMva(rawEnergy,r9,siEtaiEta,etaW,phiW,e2x2,etaSC,hoe,iso);
        else
          xgbScore = mvaEstimatorE_->computeMva(rawEnergy,r9,siEtaiEta,etaW,phiW,e2x2,etaSC,hoe,iso);
      }
      mvaScoreMap.insert(ref, xgbScore);

#ifdef DEBUG_EGAMMA_MVA

      if (scEt < mvaThresholdEt_) continue;

      float phiSC = ref->phi();
      edm::LogWarning("DiPhotonMVAMVATestProducer")
                << " xgbScore:" << xgbScore
                << " -- variables: "
                << " RawE:" << rawEnergy
                << " R9:" << r9
                << " SiEtaiEta:" << siEtaiEta
                << " EtaW:" << etaW
                << " PhiW:" << phiW
                << " S4(e2x2):" << e2x2
                << " EtaSC:" << etaSC
                << " HoE:" << hoe
                << " Iso:" << iso;

      et_->push_back(ref->et());
      scEt_->push_back(scEt);
      phi_->push_back(phiSC);
      rawEnergy_->push_back(rawEnergy);
      r9_->push_back(r9);
      siEtaiEta_->push_back(siEtaiEta);
      etaW_->push_back(etaW);
      phiW_->push_back(phiW);
      e2x2_->push_back(e2x2);
      eta_->push_back(etaSC);
      hoe_->push_back(hoe);
      iso_->push_back(iso);
      mvaScoreXGB_->push_back(xgbScore);
#endif
  }
  event.put(std::make_unique<reco::RecoEcalCandidateIsolationMap>(mvaScoreMap));

#ifdef DEBUG_EGAMMA_MVA
  if (recCollection->size())
    t_->Fill();
#endif
}

MVATestProducer::~MVATestProducer() {
#ifdef DEBUG_EGAMMA_MVA
  if (f_) f_->cd();
  if (t_) t_->Write();
  if (f_) f_->Close();
#endif
}

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestProducer);
