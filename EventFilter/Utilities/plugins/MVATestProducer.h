#ifndef EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER
#define EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER

#define DEBUG_EGAMMA_MVA //have ntuple

#include <FWCore/Framework/interface/global/EDProducer.h>
#include <FWCore/Framework/interface/one/EDProducer.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"
#include "HLTrigger/HLTcore/interface/TriggerExpressionData.h"

#include "EventFilter/Utilities/interface/PhotonMvaEstimator.h"

#include <vector>
#include <set>
#include <atomic>

class TFile;
class TTree;

namespace edm {
  class ConfigurationDescriptions;
}

namespace triggerExpression {
  class Evaluator;
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

  /// evaluator for the trigger condition
  std::vector<std::string> expressions_;
  std::vector<std::unique_ptr<triggerExpression::Evaluator>> m_expression;
  /// cache some data from the Event for faster access by the m_expression
  std::vector<triggerExpression::Data> m_eventCache;

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
  std::vector<int> *pathAccept_;
  std::vector<float> *et_;
  std::vector<float> *scEnergy_;
  std::vector<float> *scEt_;
  std::vector<float> *phi_;
  std::vector<float> *r9_;
  std::vector<float> *siEtaiEta_;
  std::vector<float> *rawEnergy_;
  std::vector<float> *etaW_;
  std::vector<float> *phiW_;
  std::vector<float> *e2x2_;
  std::vector<float> *s4_;
  std::vector<float> *eta_;
  std::vector<float> *hoe_;
  std::vector<float> *iso_;
  std::vector<float> *mvaScoreXGB_;
  std::vector<float> *xgbScoresTop2M60_;
#endif
  std::atomic<unsigned int> numPaths_ = 0;
 
};

#endif
