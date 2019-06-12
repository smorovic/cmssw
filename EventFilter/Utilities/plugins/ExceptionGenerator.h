#include "TH1D.h"

#include <FWCore/Framework/interface/MakerMacros.h>
#include <FWCore/Framework/interface/stream/EDAnalyzer.h>
#include <FWCore/Framework/interface/Event.h>
#include "FWCore/ServiceRegistry/interface/Service.h"

//#include "EventFilter/Utilities/interface/ModuleWeb.h"

#include <vector>
#include <string>
#include <map>
#include <mutex>

#include <curl/curl.h>

#include <sys/utsname.h>

namespace evf{
    class ExceptionGenerator : public edm::stream::EDAnalyzer<>
    {
    public:
      static const int menu_items = 14;
      static const std::string menu[menu_items];
						   
      explicit ExceptionGenerator( const edm::ParameterSet&);
      ~ExceptionGenerator() override{};
      void beginStream(edm::StreamID sid) override;
      void beginRun(const edm::Run& r, const edm::EventSetup& iSetup) override;
      void analyze(const edm::Event & e, const edm::EventSetup& c) override;
      void beginLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) override;
      void endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) override;

    private:
      void getLumiTestPayload(edm::Event const* e, edm::LuminosityBlock const *lb,  edm::EventSetup const *es, bool writeToFile);

      int actionId_;
      unsigned int intqualifier_;
      double qualifier2_;
      std::string qualifier_;
      bool actionRequired_;
      int sid_;
      std::string original_referrer_;
      TH1D* timingHisto_;
      timeval tv_start_;

      std::mutex stream_mutex_;
      std::map<unsigned int, bool> ls_handled_;

      std::string url_;
      struct utsname uts_;
    };
  }

