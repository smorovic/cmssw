#include "CondCore/PopCon/interface/PopConForLumiToHLT.h"
#include "CondCore/PopCon/interface/Exception.h"
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include <fstream>
#include <curl/curl.h>

namespace popcon {

  PopConForLumiToHLT::PopConForLumiToHLT(const edm::ParameterSet& pset):
    PopConBase(pset),
    m_latencyInLumisections( pset.getUntrackedParameter<unsigned int>("latency",1)),
    m_preLoadConnectionString( pset.getUntrackedParameter<std::string>("preLoadConnectionString","")),
    m_pathForLastLumiFile( pset.getUntrackedParameter<std::string>("pathForLastLumiFile","")),
    m_logFileName( pset.getUntrackedParameter<std::string>("logFileName","")),
    m_debug( pset.getUntrackedParameter<bool>( "debugLogging",false ) ){
  }

  PopConForLumiToHLT::~PopConForLumiToHLT(){
  }

  static size_t getHtmlCallback(void *contents, size_t size, size_t nmemb, void *ptr){
    // Cast ptr to std::string pointer and append contents to that string
    ((std::string*)ptr)->append((char*)contents, size * nmemb);
    return size * nmemb;
  }

  bool getLatestLumiFromDAQ( std::string& info ){
    CURL *curl;
    CURLcode res;
    std::string htmlBuffer;
    
    curl = curl_easy_init();
    bool ret = false;
    if(curl) {
      curl_easy_setopt(curl, CURLOPT_URL, "http://dvru-c2f33-27-01.cms:11600/urn:xdaq-application:lid=52/getLatestLumiSection");
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


  cond::Time_t PopConForLumiToHLT::getLastLumiProcessed(){
    cond::Time_t lastLumiProcessed = cond::time::MIN_VAL;
    if( m_pathForLastLumiFile.size() == 0 ) {
      std::string info("");
      if( !getLatestLumiFromDAQ( info ) ) throw Exception( "Can't get last Lumisection from DAQ.");
      lastLumiProcessed = boost::lexical_cast<unsigned long long>( info );
    } else {
      
      std::ifstream lastLumiFile( m_pathForLastLumiFile );
      if( lastLumiFile) { 
	lastLumiFile >> lastLumiProcessed;
      } else {
	lastLumiProcessed = 1;
      }
    }
    return lastLumiProcessed;
  }

  cond::Time_t PopConForLumiToHLT::preLoadConditions( cond::Time_t targetTime ){
    cond::persistency::ConnectionPool connection;
    //configure the connection
    if( m_debug ) {
      connection.setMessageVerbosity( coral::Debug );
    } else {
      connection.setMessageVerbosity( coral::Error );
    }
    connection.configure();
    //create the sessions
    std::string targetTimeString = std::to_string( targetTime );
    cond::persistency::Session session = connection.createReadOnlySession( m_preLoadConnectionString, targetTimeString );
    cond::persistency::TransactionScope transaction( session.transaction() );
    transaction.start(true);
    cond::persistency::IOVProxy proxy = session.readIov( tagInfo().name );
    auto iov = proxy.getInterval( targetTime );
    std::cout <<" IOV found for target "<<targetTime<<" since "<<iov.since<<" hash "<<iov.payloadId<<std::endl;
    transaction.commit();
    return iov.since;
  }

  void PopConForLumiToHLT::log( const std::string& tag, const std::string& message ){
    std::ofstream logFile( m_logFileName, std::ios_base::app );
    logFile << tag << " * " << boost::posix_time::to_simple_string( boost::posix_time::second_clock::local_time() ) <<" * "<<message<<std::endl;
    logFile.close();
  } 
    
}
