
#include <FWCore/Framework/interface/stream/EDFilter.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include <FWCore/ParameterSet/interface/ConfigurationDescriptions.h>
#include <FWCore/ParameterSet/interface/ParameterSetDescription.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include <vector>
#include <memory>

namespace edm {
  class ConfigurationDescriptions;
}

class MVATestFilter : public edm::stream::EDFilter<> {
public:
  explicit MVATestFilter(edm::ParameterSet const &);
  ~MVATestFilter() override {}

  static void fillDescriptions(edm::ConfigurationDescriptions &descriptions);

private:
  bool filter(edm::Event &, edm::EventSetup const &) override;
  edm::EDGetTokenT<trigger::TriggerFilterObjectWithRefs> inputToken_;

};


MVATestFilter::MVATestFilter(edm::ParameterSet const& config) :
      inputToken_(consumes<trigger::TriggerFilterObjectWithRefs>(config.getParameter<edm::InputTag>("inputTag")))
  {
  }

  void MVATestFilter::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.add<edm::InputTag>("inputTag");
  }

  bool MVATestFilter::filter(edm::Event& event, edm::EventSetup const& setup) {

    //prefiltered collection
    edm::Handle<trigger::TriggerFilterObjectWithRefs> PrevFilterOutput;
    event.getByToken(inputToken_, PrevFilterOutput);

    edm::LogError("MVATestFilter") << " ACCEPT ";
    return true;
  }

#include "FWCore/Framework/interface/MakerMacros.h"
DEFINE_FWK_MODULE(MVATestFilter);
