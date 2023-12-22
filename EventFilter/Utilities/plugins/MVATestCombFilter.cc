
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
  edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> mvaToken_;

};

MVATestCombFilter::MVATestCombFilter(edm::ParameterSet const& config) :
    HLTFilter(config),
    minMass_(config.getParameter<double>("minMass")),
    mvaMinBarrel_(config.getParameter<double>("mvaMinBarrel")),
    mvaMinEndcap_(config.getParameter<double>("mvaMinEndcap")),
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

  for (size_t i=0;i < recCollection->size(); i++) {
    edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

    float EtaSC = ref->eta();
    float mvaScore = (*mvaMap).find(ref)->val;

    if (fabs(EtaSC) < 1.5) {
      if (mvaScore > mvaMinBarrel_)
        p4s.emplace_back(ref->p4());
    }
    else {
      if (mvaScore > mvaMinEndcap_)
        p4s.emplace_back(ref->p4());
    }
  }

  bool accept = false;

  for (size_t first = 0; first < p4s.size(); first++) {
    for (size_t second = first + 1; second < p4s.size(); second++) {
      math::XYZTLorentzVector pairP4 = p4s[first] + p4s[second];
      double mass = pairP4.M();
      if (mass >= minMass_)
        accept = true;
    }
  }
  return accept;
}


#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestCombFilter);
