#include "CondCore/PopCon/interface/PopConForLumiToHLT.h"
#include "CondCore/PopCon/interface/Exception.h"
#include <fstream>
#include <curl/curl.h>

namespace popcon {

  PopConForLumiToHLT::PopConForLumiToHLT(const edm::ParameterSet& pset):
    PopConBase(pset),
    m_latencyInLumisections( pset.getUntrackedParameter<unsigned int>("latency",1)),
    m_runNumber( pset.getUntrackedParameter<unsigned int>("runNumber",1)),
    m_lastLumiUrl( pset.getUntrackedParameter<std::string>("lastLumiUrl","")),
    m_preLoadConnectionString( pset.getUntrackedParameter<std::string>("preLoadConnectionString","")),
    m_pathForLastLumiFile( pset.getUntrackedParameter<std::string>("pathForLastLumiFile","")),
    m_debug( pset.getUntrackedParameter<bool>( "debugLogging",false ) ){
  }

  PopConForLumiToHLT::~PopConForLumiToHLT(){
  }

  cond::persistency::Session PopConForLumiToHLT::connect( const std::string& connectionString, cond::Time_t targetTime  ){
    cond::persistency::ConnectionPool connection;
    //configure the connection                                                                                    
    if( m_debug ) {
      connection.setMessageVerbosity( coral::Debug );
    } else {
      connection.setMessageVerbosity( coral::Error );
    }
    connection.configure();
    //create the session
    cond::persistency::Session ret;
    if( targetTime != 0 ){
      std::string targetTimeString = std::to_string( targetTime );
      ret = connection.createReadOnlySession( connectionString, targetTimeString );
    } else {
      ret = connection.createSession( connectionString );
    }
    return ret;
  }

  static size_t getHtmlCallback(void *contents, size_t size, size_t nmemb, void *ptr){
    // Cast ptr to std::string pointer and append contents to that string
    ((std::string*)ptr)->append((char*)contents, size * nmemb);
    return size * nmemb;
  }

  bool getLatestLumiFromDAQ( const std::string& urlString, std::string& info ){
    CURL *curl;
    CURLcode res;
    std::string htmlBuffer;
    
    curl = curl_easy_init();
    bool ret = false;
    if(curl) {
      curl_easy_setopt(curl, CURLOPT_URL, urlString);
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, getHtmlCallback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &htmlBuffer);
      res = curl_easy_perform(curl);
      if(CURLE_OK == res) {
	info = htmlBuffer;
        ret = true;
      }
      curl_easy_cleanup(curl);
    }
    return ret;
  }

  cond::Time_t getLastLumiFromFile( const std::string& fileName ){
    if( fileName.size() == 0 ) throw Exception( "Last lumisection file path has not been provided.");
    cond::Time_t lastLumiProcessed = cond::time::MIN_VAL;
    std::ifstream lastLumiFile( fileName );
    if( lastLumiFile) { 
      lastLumiFile >> lastLumiProcessed;
    } else {
      lastLumiProcessed = 1;
    }
    return lastLumiProcessed;
  }

  cond::Time_t PopConForLumiToHLT::getLastLumiProcessed(){
    cond::Time_t lastLumiProcessed = cond::time::MIN_VAL;
    if( !m_lastLumiUrl.size() == 0 ) {
      std::string info("");
      if( !getLatestLumiFromDAQ( m_lastLumiUrl, info ) ) throw Exception( "Can't get last Lumisection from DAQ.");
      unsigned int lastLumi = boost::lexical_cast<unsigned int>( info );
      lastLumiProcessed = cond::time::lumiTime( m_runNumber, lastLumi );
    } else {
      lastLumiProcessed = getLastLumiFromFile(m_pathForLastLumiFile);
    }
    return lastLumiProcessed;
  }

}
