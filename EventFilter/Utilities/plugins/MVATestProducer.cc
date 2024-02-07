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

constexpr int NUM_COLUMNS = 9;


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
      mvaThresholdEt_(config.getParameter<double>("mvaThresholdEt"))
#ifdef DEBUG_EGAMMA_MVA
      ,rootFileName_(config.getUntrackedParameter<std::string>("treeFile", "photon_mva.root"))
#endif
{
    mvaEstimatorB_ = std::make_unique<PhotonMvaEstimator>(mvaFileXgbB_, mvaNTreeLimitB_);
    mvaEstimatorE_ = std::make_unique<PhotonMvaEstimator>(mvaFileXgbE_, mvaNTreeLimitE_);
    mvaEstimatorE_->computeMvaTest();
    produces<reco::RecoEcalCandidateIsolationMap>();

#ifdef DEBUG_EGAMMA_MVA

    scEnergy_ = new std::vector<float>();
    scEt_ = new std::vector<float>();
    phi_ = new std::vector<float>();
    r9_ = new std::vector<float>();
    siEtaiEta_ = new std::vector<float>();
    rawEnergy_ = new std::vector<float>();
    etaW_ = new std::vector<float>();
    phiW_ = new std::vector<float>();
    e2x2_ = new std::vector<float>();
    s4_ = new std::vector<float>();
    eta_ = new std::vector<float>();
    hoe_ = new std::vector<float>();
    iso_ = new std::vector<float>();
    mvaScoreXGB_ = new std::vector<float>();
    xgbScoresTop2M60_ = new std::vector<float>();
    f_ = new TFile(rootFileName_.c_str(), "RECREATE");
    t_ = new TTree("HLTPhotonMVA", "HLT Photon MVA");
    t_->Branch("eventId", &eventId_, "eventId/l");
    t_->Branch("scEnergy", "std::vector<float>", &scEnergy_);
    t_->Branch("scEt", "std::vector<float>", &scEt_);
    t_->Branch("phi", "std::vector<float>", &phi_);
    t_->Branch("r9", "std::vector<float>", &r9_);
    t_->Branch("siEtaiEta", "std::vector<float>", &siEtaiEta_);
    t_->Branch("rawEnergy", "std::vector<float>", &rawEnergy_);
    t_->Branch("etaW", "std::vector<float>", &etaW_);
    t_->Branch("phiW", "std::vector<float>", &phiW_);
    t_->Branch("e2x2", "std::vector<float>", &e2x2_);
    t_->Branch("s4", "std::vector<float>", &s4_);
    t_->Branch("eta", "std::vector<float>", &eta_);
    t_->Branch("HoE", "std::vector<float>", &hoe_);
    t_->Branch("iso", "std::vector<float>", &iso_);
    t_->Branch("mvaScoreXGB", "std::vector<float>", &mvaScoreXGB_);
    t_->Branch("mvaScoreTop2M60", "std::vector<float>", &xgbScoresTop2M60_);

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
    desc.add<double>("mvaThresholdEt", 0);
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
    scEnergy_->clear();
    scEt_->clear();
    phi_->clear();
    rawEnergy_->clear();
    r9_->clear();
    siEtaiEta_->clear();
    etaW_->clear();
    phiW_->clear();
    e2x2_->clear();
    s4_->clear();
    eta_->clear();
    hoe_->clear();
    iso_->clear();
    mvaScoreXGB_->clear();
    xgbScoresTop2M60_->clear();

    float mv1 = -1, mv2 = -1;
    int mi1 = -1, mi2 = -1;
#endif

    //output
    reco::RecoEcalCandidateIsolationMap mvaScoreMap(recCollection);

    ////if taking trigger cands
    //for (unsigned int i = 0; i < recCollection.size(); i++) {
    //  edm::Ref<reco::RecoEcalCandidateCollection> ref = recCollection[i];
    //  //edm::Ref<reco::RecoEcalCandidateCollection> ref(recoecalcands, i);

    size_t mvaCountB = 0;
    size_t mvaCountE = 0;
    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);
      if (ref->et() >= mvaThresholdEt_) {
        if (abs(ref->eta()) < 1.5) {
            mvaCountB++;
        }
        else {
            mvaCountE++;
        }
      }
    }

    std::vector<float> idxB(mvaCountB);
    std::vector<float> idxE(mvaCountE);
    //contiguous arrays
    auto varsB = std::make_unique<float[]>(mvaCountB * NUM_COLUMNS);
    auto varsE = std::make_unique<float[]>(mvaCountE * NUM_COLUMNS);

    mvaCountB = 0;
    mvaCountE = 0;

    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

      float xgbScore = -100.;
#ifdef DEBUG_EGAMMA_MVA
      mvaScoreXGB_->push_back(xgbScore);
#endif
      if (ref->et() < mvaThresholdEt_) {
          //skip
          mvaScoreMap.insert(ref, xgbScore);
          continue;
      }

      float scEnergy = ref->superCluster()->energy();
      float eInv = 1./scEnergy;
      float r9 = (*r9Map).find(ref)->val;
      float hoe = (*hoEMap).find(ref)->val * eInv;
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
      float e2x2 = (*e2x2Map).find(ref)->val;
      float s4 = e2x2 * eInv;
      float iso = (*isoMap).find(ref)->val;
      float rawEnergy = ref->superCluster()->rawEnergy();
      float etaW = ref->superCluster()->etaWidth();
      float phiW = ref->superCluster()->phiWidth();
      float etaSC = ref->eta();

      if (abs(etaSC) < 1.5) {
        const size_t rowpos = mvaCountB * NUM_COLUMNS;
        idxB[mvaCountB] = i;
        varsB[rowpos] = rawEnergy;
        varsB[rowpos + 1] = r9;
        varsB[rowpos + 2] = siEtaiEta;
        varsB[rowpos + 3] = etaW;
        varsB[rowpos + 4] = phiW;
        varsB[rowpos + 5] = s4;
        varsB[rowpos + 6] = etaSC;
        varsB[rowpos + 7] = hoe;
        varsB[rowpos + 8] = iso;
        mvaCountB++;
      } else {
        const size_t rowpos = mvaCountE * NUM_COLUMNS;
        idxE[mvaCountE] = i;
        varsE[rowpos] = rawEnergy;
        varsE[rowpos + 1] = r9;
        varsE[rowpos + 2] = siEtaiEta;
        varsE[rowpos + 3] = etaW;
        varsE[rowpos + 4] = phiW;
        varsE[rowpos + 5] = s4;
        varsE[rowpos + 6] = etaSC;
        varsE[rowpos + 7] = hoe;
        varsE[rowpos + 8] = iso;
        mvaCountE++;
      }
    }

    if (mvaCountB) {
      auto res = mvaEstimatorB_->computeMvaVec(varsB.get(), mvaCountB);
      for (size_t i = 0; i < res.size(); i++) {
        const size_t idx = idxB[i];
        edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, idx);
#ifdef DEBUG_EGAMMA_MVA
        (*mvaScoreXGB_)[idx] = res[i];
#endif
        mvaScoreMap.insert(ref, res[i]);
      }
    }
    if (mvaCountE) {
      auto res = mvaEstimatorE_->computeMvaVec(varsE.get(), mvaCountE);
      for (size_t i = 0; i < res.size(); i++) {
        const size_t idx = idxE[i];
        edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, idx);
#ifdef DEBUG_EGAMMA_MVA
        (*mvaScoreXGB_)[idx] = res[i];
#endif
        mvaScoreMap.insert(ref, res[i]);
      }
    }

#ifdef DEBUG_EGAMMA_MVA
    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

      float scEt = ref->et();

      if (scEt < mvaThresholdEt_) {
          continue;
      }
      float xgbScore = mvaScoreXGB_->at(i);

      float scEnergy = ref->superCluster()->energy();
      float eInv = 1./scEnergy;
      float r9 = (*r9Map).find(ref)->val;
      float hoe = (*hoEMap).find(ref)->val * eInv;
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
      float e2x2 = (*e2x2Map).find(ref)->val;
      float s4 = e2x2 * eInv;
      float iso = (*isoMap).find(ref)->val;
      float rawEnergy = ref->superCluster()->rawEnergy();
      float etaW = ref->superCluster()->etaWidth();
      float phiW = ref->superCluster()->phiWidth();
      float etaSC = ref->eta();
      float phiSC = ref->phi();

      edm::LogWarning("DiPhotonMVAMVATestProducer")
                << " xgbScore:" << xgbScore
                << " -- variables: "
                << " RawE:" << rawEnergy
                << " R9:" << r9
                << " SiEtaiEta:" << siEtaiEta
                << " EtaW:" << etaW
                << " PhiW:" << phiW
                << " e2x2:" << e2x2
                << " s4:" << s4
                << " EtaSC:" << etaSC
                << " HoE:" << hoe
                << " Iso:" << iso;

      scEnergy_->push_back(scEnergy);
      scEt_->push_back(scEt);
      phi_->push_back(phiSC);
      rawEnergy_->push_back(rawEnergy);
      r9_->push_back(r9);
      siEtaiEta_->push_back(siEtaiEta);
      etaW_->push_back(etaW);
      phiW_->push_back(phiW);
      e2x2_->push_back(e2x2);
      s4_->push_back(s4);
      eta_->push_back(etaSC);
      hoe_->push_back(hoe);
      iso_->push_back(iso);

      //find highest two indices
      if (scEt > 14.25 && scEt >= mvaThresholdEt_) {
        if (xgbScore > mv1) {
          mv2 = mv1;
          mi2 = mi1;
          mi1 = i;
          mv1 = xgbScore;

        } else if (xgbScore > mv2) {
          mi2 = i;
          mv2 = xgbScore;
        }
      }
#endif
  }
  event.put(std::make_unique<reco::RecoEcalCandidateIsolationMap>(mvaScoreMap));

#ifdef DEBUG_EGAMMA_MVA

  //store highest two score cands if mass is > 60  GeV
  if (mi1 != -1 && mi2 != -1) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref1(recCollection, mi1);
      edm::Ref<reco::RecoEcalCandidateCollection> ref2(recCollection, mi2);
//      auto p4 = ref1->p4() + ref2->p4();
      double mass = (ref1->p4() + ref2->p4()).M();
      if (mass > 60) {
        xgbScoresTop2M60_->push_back(mv1);
        xgbScoresTop2M60_->push_back(mv2);
      }
  }

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
