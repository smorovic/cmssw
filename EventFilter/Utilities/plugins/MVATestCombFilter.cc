
#include "HLTrigger/HLTcore/interface/HLTFilter.h"
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidate.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"

#include <vector>
#include <memory>

namespace edm {
  class ConfigurationDescriptions;
}

class MVATestCombFilter : public HLTFilter {
public:
  explicit MVATestCombFilter(edm::ParameterSet const &);
  ~MVATestCombFilter() override {}

  static void fillDescriptions(edm::ConfigurationDescriptions &descriptions);

private:
  bool hltFilter(edm::Event& event,
                 const edm::EventSetup& setup,
                 trigger::TriggerFilterObjectWithRefs& filterproduct) const override;

  //edm::EDGetTokenT<trigger::TriggerFilterObjectWithRefs> inputToken_;
  double minMass_;
  double mvaMinBarrel_;
  double mvaMinEndcap_;
  double mvaMinBarrelTight_;
  double mvaMinEndcapTight_;
  edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> mvaToken_;

};

MVATestCombFilter::MVATestCombFilter(edm::ParameterSet const& config) :
    HLTFilter(config),
    minMass_(config.getParameter<double>("minMass")),
    mvaMinBarrel_(config.getParameter<double>("mvaMinBarrel")),
    mvaMinEndcap_(config.getParameter<double>("mvaMinEndcap")),
    mvaMinBarrelTight_(config.getParameter<double>("mvaMinBarrelTight")),
    mvaMinEndcapTight_(config.getParameter<double>("mvaMinEndcapTight")),
    candToken_(consumes<reco::RecoEcalCandidateCollection>(config.getParameter<edm::InputTag>("candTag"))),
    mvaToken_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("mvaPhotonTag")))
{
}

void MVATestCombFilter::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;
  makeHLTFilterDescription(desc);
  desc.add<double>("minMass");
  desc.add<double>("mvaMinBarrel");
  desc.add<double>("mvaMinEndcap");
  desc.add<double>("mvaMinBarrelTight");
  desc.add<double>("mvaMinEndcapTight");
  desc.add<edm::InputTag>("candTag");
  desc.add<edm::InputTag>("mvaPhotonTag");
}

bool MVATestCombFilter::hltFilter(edm::Event& event,
                 const edm::EventSetup& setup,
                 trigger::TriggerFilterObjectWithRefs& filterproduct) const
{
  //producer collection (hltEgammaCandidates(Unseeded))
  edm::Handle<reco::RecoEcalCandidateCollection> recCollection;
  event.getByToken(candToken_, recCollection);

  //get hold of photon MVA association map
  edm::Handle<reco::RecoEcalCandidateIsolationMap> mvaMap;
  event.getByToken(mvaToken_, mvaMap);

  std::vector<math::XYZTLorentzVector> p4s(recCollection->size());
  std::vector<bool> isTight(recCollection->size());

  for (size_t i=0;i < recCollection->size(); i++) {
    edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

    float EtaSC = ref->eta();
    float mvaScore = (*mvaMap).find(ref)->val;

    if (fabs(EtaSC) < 1.5) {
      if (mvaScore > mvaMinBarrel_) {
        p4s.emplace_back(ref->p4());
        isTight.emplace_back(mvaScore > mvaMinBarrelTight_);
      }

    }
    else {
      if (mvaScore > mvaMinEndcap_) {
        p4s.emplace_back(ref->p4());
        isTight.emplace_back(mvaScore > mvaMinEndcapTight_);
      }
    }
  }

  bool accept = false;

  for (size_t i = 0; i < p4s.size(); i++) {
    for (size_t j = i + 1; j < p4s.size(); j++) {
      if (isTight[i] || isTight[j]) { //require one tight candidate
        math::XYZTLorentzVector pairP4 = p4s[i] + p4s[j];
        double mass = pairP4.M();
        if (mass >= minMass_)
          accept = true;
      }
    }
  }
  return accept;
}


#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestCombFilter);
