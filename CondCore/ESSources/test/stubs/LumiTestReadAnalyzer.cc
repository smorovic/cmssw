
/*----------------------------------------------------------------------

Toy EDProducers and EDProducts for testing purposes only.

----------------------------------------------------------------------*/

#include <stdexcept>
#include <string>
#include <iostream>
//#include <map>
#include "FWCore/Framework/interface/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/Framework/interface/EventSetup.h"

#include "CondFormats/DataRecord/interface/LumiTestPayloadRcd.h"
#include "CondFormats/Common/interface/LumiTestPayload.h"
#include "CondCore/CondDB/interface/Time.h"
#include <fstream>

using namespace std;

namespace edmtest
{
  class LumiTestReadAnalyzer : public edm::EDAnalyzer
  {
  public:
    explicit  LumiTestReadAnalyzer(edm::ParameterSet const& p):
      m_pathForLastLumiFile( p.getUntrackedParameter<std::string>("pathForLastLumiFile","")),
      m_pathForAllLumiFile( p.getUntrackedParameter<std::string>("pathForAllLumiFile",""))
    { 
    }
    explicit  LumiTestReadAnalyzer(int i) 
    { 
    }
    virtual ~LumiTestReadAnalyzer() {  
    }
    virtual void beginJob();
    virtual void beginRun(const edm::Run&, const edm::EventSetup& context);
    virtual void analyze(const edm::Event& e, const edm::EventSetup& c);
  private:
    std::string m_pathForLastLumiFile;
    std::string m_pathForAllLumiFile;
  };
  void
  LumiTestReadAnalyzer::beginRun(const edm::Run&, const edm::EventSetup& context){
  }
  void
  LumiTestReadAnalyzer::beginJob(){
  }
  void
  LumiTestReadAnalyzer::analyze(const edm::Event& e, const edm::EventSetup& context){
    if( m_pathForLastLumiFile.size() > 0 ) {
      cond::Time_t prevTimeProcessed = 0;
      {
	std::ifstream lastTimeFile( m_pathForLastLumiFile );
	if( !lastTimeFile.fail() ) lastTimeFile >> prevTimeProcessed;
      }
      cond::Time_t currentTimeProcessed = e.id().run();
      currentTimeProcessed = ( currentTimeProcessed << 32 ) + e.id().luminosityBlock();
      if( currentTimeProcessed > prevTimeProcessed ){
	std::ofstream lastTimeFile( m_pathForLastLumiFile );
	lastTimeFile << currentTimeProcessed;
	lastTimeFile.close();
        if( m_pathForAllLumiFile.size() > 0 ) {
	  std::ofstream allTimeFile( m_pathForAllLumiFile, std::ios_base::app );
	  allTimeFile << currentTimeProcessed << "  " << boost::posix_time::to_simple_string( boost::posix_time::second_clock::local_time() ) <<std::endl;
	  allTimeFile.close();
	}
      }
    }
    //
    edm::eventsetup::EventSetupRecordKey recordKey(edm::eventsetup::EventSetupRecordKey::TypeTag::findType("LumiTestPayloadRcd"));
    if( recordKey.type() == edm::eventsetup::EventSetupRecordKey::TypeTag()) {
      //record not found
      std::cout <<"Record \"LumiTestPayloadRcd"<<"\" does not exist "<<std::endl;
    }
    edm::ESHandle<cond::LumiTestPayload> ps;
    context.get<LumiTestPayloadRcd>().get(ps);
    const cond::LumiTestPayload* payload=ps.product();
    std::cout<<"Event "<<e.id().event()<<" Run "<<e.id().run()<<" Lumi "<<e.id().luminosityBlock()<<" LumiTestPayload "<<payload->m_id<<std::endl;
    sleep( 10 );
  }
  DEFINE_FWK_MODULE(LumiTestReadAnalyzer);
}
