#include <memory>

#include "TestProducer.h"

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "DataFormats/RecoCandidate/interface/RecoEcalCandidate.h"
#include "DataFormats/EgammaReco/interface/SuperCluster.h"
#include "DataFormats/EgammaReco/interface/SuperClusterFwd.h"
#include "DataFormats/Common/interface/AssociationMap.h"
//#include "DataFormats/RecoCandidate/interface/RecoCandidate.h"


  TestProducer::TestProducer(edm::ParameterSet const& config) :
      candToken_(consumes<trigger::TriggerFilterObjectWithRefs>(config.getParameter<edm::InputTag>("candTag"))),
      tokenR9_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagR9"))), //cms.InputTag( 'hltEgammaR9IDUnseeded','r95x5' )
      tokenHoE_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagHoE"))), //cms.InputTag( "hltEgammaHoverE" )
      tokenSigmaiEtaiEta_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagSigmaiEtaiEta"))), //('hltEgammaClusterShapeUnseeded','sigmaIEtaIEta5x5NoiseCleaned' or 'sigmaIEtaIEta5x5' only)
      tokenIso_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("inputTagIso"))) //cms.InputTag( "hltEgammaEcalPFClusterIsoUnseeded" ) or cms.InputTag( "hltEgammaHollowTrackIsoUnseeded" ) ?
  {

    produces<std::vector<float>>();
  }

  void TestProducer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.add<edm::InputTag>("candTag");
    desc.add<edm::InputTag>("inputTagR9");
    desc.add<edm::InputTag>("inputTagHoE");
    desc.add<edm::InputTag>("inputTagSigmaiEtaiEta");
  }

  void TestProducer::produce(edm::StreamID sid, edm::Event& event, edm::EventSetup const& setup) const {

    // Ref to Candidate object to be recorded in filter object
    edm::Ref<reco::RecoEcalCandidateCollection> ref;

    //prefiltered collection
    edm::Handle<trigger::TriggerFilterObjectWithRefs> PrevFilterOutput;
    event.getByToken(candToken_, PrevFilterOutput);

    //all cands?
    std::vector<edm::Ref<reco::RecoEcalCandidateCollection> > recoecalcands;
    //PrevFilterOutput->getObjects(TriggerCluster, recoecalcands);
    //if (recoecalcands.empty())
    //probably this collection
    PrevFilterOutput->getObjects(trigger::TriggerPhoton, recoecalcands);

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


    // look at all photons, check cuts and add to filter object
    for (unsigned int i = 0; i < recoecalcands.size(); i++) {
      ref = recoecalcands[i];

      float EtaSC = ref->eta();
      float PhiSC = ref->phi();

      //reco::RecoEcalCandidateIsolationMap::const_iterator r9i = (*r9Map).find(ref);
      float r9 = (*r9Map).find(ref)->val;
      float hoe = (*hoEMap).find(ref)->val;
      float siEtaiEta = (*sigmaiEtaiEtaMap).find(ref)->val;
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

      //TODO: calculate S4 -> possibly make a new egamma producer and/or implement S4 in EcalLazyTools which needs access to rechits

      //TODO: do MVA
      std::cout << EtaSC << " " << PhiSC << " " << r9 << " " << hoe << " " << siEtaiEta << " " << iso << " " << rawE << " " << etaW << " " << phiW << " " << scEt << std::endl;

      //TODO :nsert here MVA result
      std::unique_ptr<std::vector<float>> pi(new std::vector<float>());
      event.put(std::move(pi));
  }

}  // namespace evf
