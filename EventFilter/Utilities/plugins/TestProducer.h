#ifndef EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER
#define EVENTFILTER_UTILITIES_PLUGINS_TESTPRODUCER

#include <vector>

#include <FWCore/Framework/interface/stream/EDProducer.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"

#include "DataFormats/EcalRecHit/interface/EcalRecHitCollections.h"
#include "RecoEcal/EgammaCoreTools/interface/EcalClusterLazyTools.h"
namespace edm {
  class ConfigurationDescriptions;
}

class MVATestProducer : public edm::stream::EDProducer<> {
public:
  explicit MVATestProducer(edm::ParameterSet const &);
  ~MVATestProducer() override {}

  static void fillDescriptions(edm::ConfigurationDescriptions &descriptions);

private:
  void produce(edm::Event &, edm::EventSetup const &) override;

  //edm::EDGetTokenT<trigger::TriggerFilterObjectWithRefs> candToken_; //use if reading from a filter
  edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_; //use if reading from a producer
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenR9_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenHoE_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenSigmaiEtaiEta_;
  edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> tokenIso_;

  const edm::EDGetTokenT<EcalRecHitCollection> ecalRechitEBToken_;
  const edm::EDGetTokenT<EcalRecHitCollection> ecalRechitEEToken_;
  const EcalClusterLazyTools::ESGetTokens ecalClusterToolsESGetTokens_;

};

#endif
