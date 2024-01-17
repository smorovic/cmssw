#ifndef EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER
#define EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER

//#define DEBUG_EGAMMA_MVA //have ntuple - note: remove this line for timing tests

#include <FWCore/Framework/interface/global/EDProducer.h>
#include <FWCore/Framework/interface/one/EDProducer.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"

#include "EventFilter/Utilities/interface/PhotonMvaEstimator.h"

#include <vector>

class TFile;
class TTree;

namespace edm {
  class ConfigurationDescriptions;
}

#ifdef DEBUG_EGAMMA_MVA
class MVATestProducer : public edm::one::EDProducer<> {
#else
class MVATestProducer : public edm::global::EDProducer<> {
#endif
public:
  explicit MVATestProducer(edm::ParameterSet const &);
  ~MVATestProducer();

  static void fillDescriptions(edm::ConfigurationDescriptions &descriptions);

private:
#ifdef DEBUG_EGAMMA_MVA
  void produce(edm::Event &, edm::EventSetup const &) override;
#else
  void produce(edm::StreamID, edm::Event &, edm::EventSetup const &) const override;
#endif

  //edm::EDGetTokenT<trigger::TriggerFilterObjectWithRefs> candToken_; //use if reading from a filter

  edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_; //use if reading from a producer
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenR9_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenHoE_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenSigmaiEtaiEta_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenE2x2_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenIso_;

  const edm::FileInPath mvaFileXgbB_;
  const edm::FileInPath mvaFileXgbE_;
  unsigned mvaNTreeLimitB_ = 0;
  unsigned mvaNTreeLimitE_ = 0;
  double mvaThresholdEt_ = 0;

  std::unique_ptr<PhotonMvaEstimator> mvaEstimatorB_;
  std::unique_ptr<PhotonMvaEstimator> mvaEstimatorE_;

#ifdef DEBUG_EGAMMA_MVA
  std::string rootFileName_;
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
  std::vector<float> *mvaScoreXGB_;
#endif
 
};

#endif
