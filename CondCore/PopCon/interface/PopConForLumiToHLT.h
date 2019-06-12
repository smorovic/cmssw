#ifndef POPCON_POPCONFORLUMITOHLT_H
#define POPCON_POPCONFORLUMITOHLT_H
//
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include "CondCore/PopCon/interface/PopCon.h"
#include "CondCore/PopCon/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <chrono>
#include <iostream>

namespace popcon {


  /* Populator of the Condition DB
   * Implementation specific for Lumi-Based condition targeted to HLT
   */
  class PopConForLumiToHLT: public PopConBase {
  public:
    PopConForLumiToHLT(const edm::ParameterSet& pset);
     
    virtual ~PopConForLumiToHLT();

    template<typename Source>
    void write(Source const & source);
     
  protected:
    cond::Time_t getLastLumiProcessed();
    template<typename PayloadType>
    cond::Time_t preLoadConditions( const std::string& connectionString, cond::Time_t targetTime );

  private:
    cond::persistency::Session connect( const std::string& connectionString, cond::Time_t targetTime = 0);
    template<typename PayloadType>
    cond::Time_t doPreLoadConditions( cond::persistency::Session& session, cond::Time_t targetTime );

  private:
    static constexpr const char* const MSGSOURCE = "PopConForLumiToHLT";
    size_t m_latencyInLumisections;
    unsigned int m_runNumber;
    std::string m_lastLumiUrl;
    std::string m_preLoadConnectionString;
    std::string m_pathForLastLumiFile;
    bool m_debug;
  };

  template<typename PayloadType>
  cond::Time_t PopConForLumiToHLT::preLoadConditions( const std::string& connectionString, cond::Time_t targetTime ){
    cond::persistency::Session session = connect( connectionString, targetTime );
    return doPreLoadConditions<PayloadType>( session, targetTime );
  }

  template<typename PayloadType>
  cond::Time_t PopConForLumiToHLT::doPreLoadConditions( cond::persistency::Session& session, cond::Time_t targetTime ){
    cond::persistency::TransactionScope transaction( session.transaction() );
    transaction.start(true);
    cond::persistency::IOVProxy proxy = session.readIov( tagInfo().name );
    auto iov = proxy.getInterval( targetTime );
    auto payload = session.fetchPayload<PayloadType>( iov.payloadId );
    std::cout <<" connection: "<<session.connectionString()<<" target: "<<targetTime<<" since: "<<iov.since<<" payload: "<<payload->m_id<<std::endl;
    if( payload->m_id != targetTime ) {
      std::stringstream msg;
      msg <<"Expected payload id:"<<targetTime<<" found:"<< payload->m_id;
      std::cout << msg.str() <<std::endl;
      //throw Exception( msg.str() );
    }
    transaction.commit();
    return iov.since;
  }

  template<typename Source>
  void PopConForLumiToHLT::write(Source const & source) {
    typedef typename Source::value_type PayloadType;
    typedef typename Source::Container Container;
    
    cond::persistency::Session mainSession = initialize();
    // remove me
    //std::string oracleConnectionString = mainSession.connectionString();
    // 
    std::pair<Container const *, std::string const> ret = source(mainSession,
  								 tagInfo(),logDBEntry()); 
    Container const & payloads = *ret.first;
    
    if(PopConBase::isLoggingOn()) {
      std::ostringstream s;
      s << "PopCon v" << s_version << "; " << displayIovHelper(payloads) << ret.second;
      PopConBase::setLogHeader( source.id(),s.str() );
    }
    displayHelper(payloads);
    cond::Time_t lastSince = PopConBase::tagInfo().lastInterval.since;
    if( lastSince == cond::time::MAX_VAL ) lastSince = 0;
    cond::Time_t targetTime = getLastLumiProcessed()+m_latencyInLumisections;
    edm::LogInfo( MSGSOURCE ) << "Found "<< payloads.size() << " payload(s) in the queue.";
    edm::LogInfo( MSGSOURCE ) << "Last since: "<<lastSince<<" target since: "<<targetTime;
    if( payloads.size() > 0 &&
	// this check is not required if the policy update is kept as the general for synch=offline, express, primpt, hlt, mc
	targetTime > lastSince ){
      auto p = payloads.back();
      //auto t0 = std::chrono::high_resolution_clock::now();
      // insert the new lumisection...
      /** remove me - this is only for the cond::LumiTestPayload class **/
      edm::LogInfo( MSGSOURCE ) << "Attaching id "<< targetTime  << " to the payload(s).";
      const_cast<PayloadType*>(p.first)->m_id = targetTime; 
      cond::Hash payloadId = PopConBase::writeOne<PayloadType>( p.first, targetTime );
      PopConBase::finalize();
      //auto t1 = std::chrono::high_resolution_clock::now();
      // check for late updates...
      cond::Time_t lastProcessed = getLastLumiProcessed();
      edm::LogInfo( MSGSOURCE ) << "Last lumisection processed after update: "<<lastProcessed;
      // check the pre-loaded iov
      edm::LogInfo( MSGSOURCE ) << "Preloading lumisection "<<targetTime;
      //auto t2 = std::chrono::high_resolution_clock::now();
      ::sleep( 2 );
      //std::string oracleConnectionString = mainSession.connectionString();
      //cond::persistency::Session oracleSession = connect( oracleConnectionString, targetTime );
      //doPreLoadConditions<PayloadType>( oracleSession, targetTime );
      cond::Time_t sinceTime = preLoadConditions<PayloadType>( m_preLoadConnectionString, targetTime );
      {
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
	std::ofstream logFile( "lumi_write.txt", std::ios_base::app );
        logFile << "Time "<<boost::posix_time::to_iso_extended_string(now)<<" Target "<<targetTime<<" hash "<<payloadId<<" Preload since "<<sinceTime<<std::endl;
      }
      //auto t3 = std::chrono::high_resolution_clock::now();
      //cond::persistency::Session frontierSession = connect( m_preLoadConnectionString );
      //doPreLoadConditions<PayloadType>( frontierSession, targetTime );
      //std::string oracleConnectionString = mainSession.connectionString(); 
      //cond::persistency::Session oracleSession = connect( oracleConnectionString, targetTime );                                     
      //doPreLoadConditions<PayloadType>( oracleSession, targetTime );                                                                  
      /**
      {
	preLoadConditions<PayloadType>( m_preLoadConnectionString, targetTime );
	auto t4 = std::chrono::high_resolution_clock::now();
	cond::persistency::Session oracleSession = connect( oracleConnectionString, targetTime );
	doPreLoadConditions<PayloadType>( oracleSession, targetTime );
	auto t5 = std::chrono::high_resolution_clock::now();
	doPreLoadConditions<PayloadType>( oracleSession, targetTime );
	auto t6 = std::chrono::high_resolution_clock::now();
	auto wlatency = std::chrono::duration_cast<std::chrono::microseconds>( t1 - t0 ).count();
	auto r0_latency = std::chrono::duration_cast<std::chrono::microseconds>( t3 - t2 ).count();
	auto r1_latency = std::chrono::duration_cast<std::chrono::microseconds>( t4 - t3 ).count();
	auto wrlatency = std::chrono::duration_cast<std::chrono::microseconds>( t3 - t0 ).count();
	auto or0_latency = std::chrono::duration_cast<std::chrono::microseconds>( t5 - t4 ).count();
	auto or1_latency = std::chrono::duration_cast<std::chrono::microseconds>( t6 - t5 ).count();
	std::stringstream ss;
	ss << " "<<std::to_string(targetTime)<<" w:"<<std::to_string( wlatency ) <<" wr:"<< std::to_string( wrlatency );
	log( "upload_full", ss.str() );
        ss.str(std::string());
        ss << " "<<std::to_string(targetTime)<<" r0:"<<std::to_string( r0_latency ) <<" r1:"<< std::to_string( r1_latency );
	log( "preload_frontier", ss.str() );
        ss.str(std::string());
        ss << " "<<std::to_string(targetTime)<<" or0:"<<std::to_string( or0_latency ) <<" or1:"<< std::to_string( or1_latency );
	log( "preload_oracle", ss.str() );
      }
      **/
      edm::LogInfo( MSGSOURCE ) << "Since time found for target lumi ("<<targetTime<<"): "<<sinceTime;
      cond::Time_t lastProcessedAfterUpdate = getLastLumiProcessed();
      if( lastProcessedAfterUpdate >= targetTime ){
	std::stringstream msg;
	msg <<"Update for target lumisection "<<targetTime<<" has been completed late: current lumisection is "<< lastProcessedAfterUpdate;
	throw Exception( msg.str() );
      }
      if( sinceTime < targetTime ){
	std::stringstream msg;
	msg << "Preload failed: expected since "<<targetTime<<"; found "<<sinceTime<<std::endl;
	throw Exception( msg.str() ); 
	// delete the new iov...
	//PopConBase::eraseIov( payloadId, targetTime ); 
        // for the moment, only detect the problem...
	//edm::LogWarning( MSGSOURCE ) << "Found a late update. A revert was required.";
	//PopConBase::finalize();
      } 
    } else {
      std::cout <<"### nothing to do: payloads "<<payloads.size()<<" targetTime "<<targetTime<<" lastSince "<<lastSince<<std::endl;
    }
  }
 
}

#endif //  POPCON_POPCONFORLUMITOHLT_H


