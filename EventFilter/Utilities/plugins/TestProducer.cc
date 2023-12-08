#include <memory>

#include "TestProducer.h"

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "DataFormats/RecoCandidate/interface/RecoEcalCandidate.h"
#include "DataFormats/EgammaReco/interface/SuperCluster.h"
#include "DataFormats/EgammaReco/interface/SuperClusterFwd.h"
#include "DataFormats/Common/interface/AssociationMap.h"


#include "FWCore/MessageLogger/interface/MessageLogger.h"

MVATestProducer::MVATestProducer(edm::ParameterSet const& config) :
      //if getting cands from a filter
      //candToken_(consumes<trigger::TriggerFilterObjectWithRefs>(config.getParameter<edm::InputTag>("candTag"))), //if we consume filter output
      //else
      candToken_(consumes<reco::RecoEcalCandidateCollection>(config.getParameter<edm::InputTag>("candTag"))), //if we consume a producer
      //endif
      tokenR9_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagR9"))),
//        edm::InputTag("hltEgammaR9IDUnseeded", "r95x5"))),
      tokenHoE_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagHoE"))),
//        edm::InputTag("hltEgammaHoverE"))),
      tokenSigmaiEtaiEta_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagSigmaiEtaiEta"))),
//        edm::InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"))),
      tokenIso_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagIso"))),
//       edm::InputTag( "hltEgammaEcalPFClusterIsoUnseeded" )))// or cms.InputTag( "hltEgammaHollowTrackIsoUnseeded" )
      ecalRechitEBToken_(consumes(config.getParameter<edm::InputTag>("ecalRechitEB"))), //will move to ClusterShapeProducer
      ecalRechitEEToken_(consumes(config.getParameter<edm::InputTag>("ecalRechitEE"))),
      ecalClusterToolsESGetTokens_(consumesCollector())
  {
    produces<std::vector<float>>();
  }

  void MVATestProducer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.add<edm::InputTag>("candTag");
    desc.add<edm::InputTag>("inputTagR9", edm::InputTag("hltEgammaR9IDUnseeded", "r95x5"));
    desc.add<edm::InputTag>("inputTagHoE", edm::InputTag("hltEgammaHoverEUnseeded"));
    desc.add<edm::InputTag>("inputTagSigmaiEtaiEta", edm::InputTag("hltEgammaClusterShapeUnseeded", "sigmaIEtaIEta5x5NoiseCleaned"));
    desc.add<edm::InputTag>("inputTagIso", edm::InputTag("hltEgammaEcalPFClusterIsoUnseeded"));
  }

  void MVATestProducer::produce(edm::Event& event, edm::EventSetup const& setup) {

    //ifdef we get cands from a filter
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


    //else
    edm::Handle<reco::RecoEcalCandidateCollection> recCollection; // "hltEgammaCandidatesUnseeded"
    event.getByToken(candToken_, recCollection);

    //endif



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
    edm::Handle<reco::RecoEcalCandidateIsolationMap> isoMap;
    event.getByToken(tokenIso_, isoMap);

    auto const& ecalClusterToolsESData = ecalClusterToolsESGetTokens_.get(setup);
    EcalClusterLazyTools lazyTools(event, ecalClusterToolsESData, ecalRechitEBToken_, ecalRechitEEToken_);

    // look at all photons, check cuts and add to filter object
    //



    //if taking trigger cands
//    for (unsigned int i = 0; i < recoecalcands.size(); i++) {
//      ref = recoecalcands[i];
//      //edm::Ref<reco::RecoEcalCandidateCollection> ref(recoecalcands, i);?
//
//
//  #else
    for (size_t i=0;i < recCollection->size(); i++) {
      edm::Ref<reco::RecoEcalCandidateCollection> ref(recCollection, i);

    //endif

      edm::LogWarning("MVATestProducer") << " cand 1";

      float EtaSC = ref->eta();
      float PhiSC = ref->phi();

      //reco::RecoEcalCandidateIsolationMap::const_iterator r9i = (*r9Map).find(ref);
      edm::LogWarning("MVATestProducer") << " cand 2";
      float r9 = (*r9Map).find(ref)->val;
      edm::LogWarning("MVATestProducer") << " cand 3";
      float hoe = (*hoEMap).find(ref)->val;
      edm::LogWarning("MVATestProducer") << " cand 4";
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
      edm::LogWarning("MVATestProducer") << " cand 5";
      float iso = (*isoMap).find(ref)->val;
      edm::LogWarning("MVATestProducer") << " cand 5";

      float rawE = ref->superCluster()->rawEnergy();
      float etaW = ref->superCluster()->etaWidth();
      float phiW = ref->superCluster()->phiWidth();
      edm::LogWarning("MVATestProducer") << " cand 6";

      float scEnergy = ref->superCluster()->energy();
      float scEt = scEnergy * sin(2 * atan(exp(-EtaSC)));
      if (scEnergy < 0.)
        scEnergy = 0.;
      if (scEt < 0.)
        scEt = 0.; /* first and second order terms assume non-negative energies */

      edm::LogWarning("MVATestProducer") << " cand 7";
      //calculate maximum energy 2x2 cluster in 3x3
      float s4 = lazyTools.s4(*(ref->superCluster()->seed()));
      edm::LogWarning("MVATestProducer") << " cand 8";

      //TODO: calculate S4 -> possibly make a new egamma producer and/or implement S4 in EcalLazyTools which needs access to rechits

      //TODO: do MVA calculation with cand pairs
      edm::LogWarning("DiphotonMVAMVATestProducer") << EtaSC << " " << PhiSC << " " << r9 << " " << hoe << " "
	        << siEtaiEta << " " << iso << " " << rawE << " " << etaW << " "
		<< phiW << " " << scEt << " " << s4;
    }
      edm::LogWarning("DiphotonMVAMVATestProducer") << "END";

    //TODO :nsert here MVA results into a vector pushed here
    std::unique_ptr<std::vector<float>> pi(new std::vector<float>());
    event.put(std::move(pi));

  }

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestProducer);
