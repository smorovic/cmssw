
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
    double highMassCut_;
    std::vector<double> leadCutHighMass1_;
    std::vector<double> subCutHighMass1_;
    std::vector<double> leadCutHighMass2_;
    std::vector<double> subCutHighMass2_;
    std::vector<double> leadCutHighMass3_;
    std::vector<double> subCutHighMass3_;

    double lowMassCut_;
    std::vector<double> leadCutLowMass1_;
    std::vector<double> subCutLowMass1_;
    std::vector<double> leadCutLowMass2_;
    std::vector<double> subCutLowMass2_;
    std::vector<double> leadCutLowMass3_;
    std::vector<double> subCutLowMass3_;

    edm::EDGetTokenT<reco::RecoEcalCandidateCollection> candToken_;
    edm::EDGetTokenT<reco::RecoEcalCandidateIsolationMap> mvaToken_;

};

MVATestCombFilter::MVATestCombFilter(edm::ParameterSet const& config) :
    HLTFilter(config),
    highMassCut_(config.getParameter<double>("highMassCut")),
    leadCutHighMass1_(config.getParameter<std::vector<double>>("leadCutHighMass1")),
    subCutHighMass1_(config.getParameter<std::vector<double>>("subCutHighMass1")),
    leadCutHighMass2_(config.getParameter<std::vector<double>>("leadCutHighMass2")),
    subCutHighMass2_(config.getParameter<std::vector<double>>("subCutHighMass2")),
    leadCutHighMass3_(config.getParameter<std::vector<double>>("leadCutHighMass3")),
    subCutHighMass3_(config.getParameter<std::vector<double>>("subCutHighMass3")),

    lowMassCut_(config.getParameter<double>("lowMassCut")),
    leadCutLowMass1_(config.getParameter<std::vector<double>>("leadCutLowMass1")),
    subCutLowMass1_(config.getParameter<std::vector<double>>("subCutLowMass1")),
    leadCutLowMass2_(config.getParameter<std::vector<double>>("leadCutLowMass2")),
    subCutLowMass2_(config.getParameter<std::vector<double>>("subCutLowMass2")),
    leadCutLowMass3_(config.getParameter<std::vector<double>>("leadCutLowMass3")),
    subCutLowMass3_(config.getParameter<std::vector<double>>("subCutLowMass3")),

    candToken_(consumes<reco::RecoEcalCandidateCollection>(config.getParameter<edm::InputTag>("candTag"))),
    mvaToken_(consumes<reco::RecoEcalCandidateIsolationMap>(config.getParameter<edm::InputTag>("mvaPhotonTag")))
{
}

void MVATestCombFilter::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    makeHLTFilterDescription(desc);

    desc.add<double>("highMassCut");
    desc.add<std::vector<double>>("leadCutHighMass1");
    desc.add<std::vector<double>>("subCutHighMass1");
    desc.add<std::vector<double>>("leadCutHighMass2");
    desc.add<std::vector<double>>("subCutHighMass2");
    desc.add<std::vector<double>>("leadCutHighMass3");
    desc.add<std::vector<double>>("subCutHighMass3");

    desc.add<double>("lowMassCut");
    desc.add<std::vector<double>>("leadCutLowMass1");
    desc.add<std::vector<double>>("subCutLowMass1");
    desc.add<std::vector<double>>("leadCutLowMass2");
    desc.add<std::vector<double>>("subCutLowMass2");
    desc.add<std::vector<double>>("leadCutLowMass3");
    desc.add<std::vector<double>>("subCutLowMass3");

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

    bool accept = false;

    for (size_t i=0;i < recCollection->size(); i++) {
        edm::Ref<reco::RecoEcalCandidateCollection> refi(recCollection, i);
        float EtaSCi = refi->eta();
        int eta1;
        if (fabs(EtaSCi) < 1.5) eta1 = 0;
        else eta1 = 1;
        float mvaScorei = (*mvaMap).find(refi)->val;
        math::XYZTLorentzVector p4i = refi->p4();
        for (size_t j=i+1;j < recCollection->size(); j++) {
            edm::Ref<reco::RecoEcalCandidateCollection> refj(recCollection, j);
            float EtaSCj = refj->eta();
            int eta2;
            if (fabs(EtaSCj) < 1.5) eta2 = 0;
            else eta2 = 1;
            float mvaScorej = (*mvaMap).find(refj)->val;
            math::XYZTLorentzVector p4j = refj->p4();
            math::XYZTLorentzVector pairP4 = p4i + p4j;
            double mass = pairP4.M();
            if(mass >= highMassCut_){
                if (mvaScorei >= mvaScorej &&
                   ((mvaScorei > leadCutHighMass1_.at(eta1) && mvaScorej > subCutHighMass1_.at(eta2))
                    ||(mvaScorei > leadCutHighMass2_.at(eta1) && mvaScorej > subCutHighMass2_.at(eta2))
                    ||(mvaScorei > leadCutHighMass3_.at(eta1) && mvaScorej > subCutHighMass3_.at(eta2)))){
                        accept = true;
                }//if scoreI > scoreJ
                else if (mvaScorej > mvaScorei &&
                         ((mvaScorej > leadCutHighMass1_.at(eta1) && mvaScorei > subCutHighMass1_.at(eta2))
                         ||(mvaScorej > leadCutHighMass2_.at(eta1) && mvaScorei > subCutHighMass2_.at(eta2))
                         ||(mvaScorej > leadCutHighMass3_.at(eta1) && mvaScorei > subCutHighMass3_.at(eta2)))){
                    accept = true;
                }// if scoreJ > scoreI
            }//If high mass
            else if(mass > lowMassCut_ && mass < highMassCut_){
                if (mvaScorei >= mvaScorej &&
                    ((mvaScorei > leadCutLowMass1_.at(eta1) && mvaScorej > subCutLowMass1_.at(eta2))
                    ||(mvaScorei > leadCutLowMass2_.at(eta1) && mvaScorej > subCutLowMass2_.at(eta2))
                    ||(mvaScorei > leadCutLowMass3_.at(eta1) && mvaScorej > subCutLowMass3_.at(eta2)))){
                    accept = true;
                }//if scoreI > scoreJ
                else if (mvaScorej > mvaScorei &&
                         ((mvaScorej > leadCutLowMass1_.at(eta1) && mvaScorei > subCutLowMass1_.at(eta2))
                         ||(mvaScorej > leadCutLowMass2_.at(eta1) && mvaScorei > subCutLowMass2_.at(eta2))
                         ||(mvaScorej > leadCutLowMass3_.at(eta1) && mvaScorei > subCutLowMass3_.at(eta2)))){
                    accept = true;
                }//if scoreJ > scoreI
            }//If low mass
        }//j loop
    }//i loop
  return accept;
}


#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestCombFilter);

