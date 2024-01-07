#ifndef EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER
#define EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER


#include <FWCore/Framework/interface/stream/EDProducer.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"

#include "EventFilter/Utilities/interface/photonMvaEstimator.h"

#include <vector>

class TFile;
class TTree;

namespace edm {
  class ConfigurationDescriptions;
}

class MVATestProducer : public edm::stream::EDProducer<> {
public:
  explicit MVATestProducer(edm::ParameterSet const &);
  ~MVATestProducer();

  static void fillDescriptions(edm::ConfigurationDescriptions &descriptions);

private:
  void produce(edm::Event &, edm::EventSetup const &) override;

  //edm::EDGetTokenT<trigger::TriggerFilterObjectWithRefs> candToken_; //use if reading from a filter
  edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_; //use if reading from a producer
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenR9_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenHoE_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenSigmaiEtaiEta_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenE2x2_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenIso_;

  const edm::FileInPath mvaFileB_;
  const edm::FileInPath mvaFileE_;
  const edm::FileInPath mvaFileXgbB_;
  const edm::FileInPath mvaFileXgbE_;

  std::unique_ptr<const photonMvaEstimator> mvaEstimatorB_;
  std::unique_ptr<const photonMvaEstimator> mvaEstimatorE_;

  TFile *f_ = nullptr;
  TTree *t_ = nullptr;

  uint64_t eventId_ = 0;
  std::vector<float> *et_;
  std::vector<float> *scEt_;
  std::vector<float> *phi_;
  std::vector<float> *r9_;
  std::vector<float> *siEtaiEta_;
  std::vector<float> *rawEnergy_;
  std::vector<float> *etaW_;
  std::vector<float> *phiW_;
  std::vector<float> *e2x2_;
  std::vector<float> *eta_;
  std::vector<float> *hoe_;
  std::vector<float> *iso_;
  std::vector<float> *mvaScore_;
  std::vector<float> *mvaScoreXGB_;
 
};

#endif
