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
    unsigned int m_writeTransactionDelay = 0;
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
    if( payload->m_id != targetTime ) {
      edm::LogWarning( MSGSOURCE ) <<"Expected payload id:"<<targetTime<<" found:"<< payload->m_id;
    }
    transaction.commit();
    return iov.since;
  }

  template<typename Source>
  void PopConForLumiToHLT::write(Source const & source) {
    typedef typename Source::value_type PayloadType;
    typedef typename Source::Container Container;
    
    cond::persistency::Session mainSession = initialize();
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
      auto t0 = std::chrono::high_resolution_clock::now();
      // insert the new lumisection...
      /***** remove me - this is only for the cond::LumiTestPayload class **/
      edm::LogInfo( MSGSOURCE ) << "Attaching id "<< targetTime  << " to the payload(s).";
      const_cast<PayloadType*>(p.first)->m_id = targetTime;
      /*********************************************************************/ 
      edm::LogInfo( MSGSOURCE ) << "Updating lumisection "<<targetTime;
      cond::Hash payloadId = PopConBase::writeOne<PayloadType>( p.first, targetTime );
      if( m_writeTransactionDelay ){
	edm::LogWarning( MSGSOURCE ) << "Waiting "<<m_writeTransactionDelay<<" before commit the changes...";
	::sleep( m_writeTransactionDelay );
      }
      PopConBase::finalize();
      auto t1 = std::chrono::high_resolution_clock::now();
      auto w_lat = std::chrono::duration_cast<std::chrono::microseconds>( t1 - t0 ).count();
      edm::LogInfo( MSGSOURCE ) << "Update has taken "<< w_lat <<" microsecs.";
      // check for late updates...
      cond::Time_t lastProcessed = getLastLumiProcessed();
      edm::LogInfo( MSGSOURCE ) << "Last lumisection processed after update: "<<lastProcessed;
      // check the pre-loaded iov
      edm::LogInfo( MSGSOURCE ) << "Preloading lumisection "<<targetTime;
      auto t2 = std::chrono::high_resolution_clock::now();
      cond::Time_t sinceTime = preLoadConditions<PayloadType>( m_preLoadConnectionString, targetTime );
      auto t3 = std::chrono::high_resolution_clock::now();
      edm::LogInfo( MSGSOURCE ) << "Iov for preloaded lumisection "<<targetTime<<" is "<<sinceTime;
      auto p_lat = std::chrono::duration_cast<std::chrono::microseconds>( t3 - t2 ).count();
      edm::LogInfo( MSGSOURCE ) << "Preload has taken "<< p_lat <<" microsecs.";
      if( sinceTime < targetTime ){
	edm::LogWarning( MSGSOURCE ) << "Found a late update for lumisection "<<targetTime<<"(found since "<<sinceTime<<"). A revert is required.";
	PopConBase::eraseIov( payloadId, targetTime );
	PopConBase::finalize();
      }
      auto t4 = std::chrono::high_resolution_clock::now();
      auto t_lat = std::chrono::duration_cast<std::chrono::microseconds>( t4 - t0 ).count();
      edm::LogInfo( MSGSOURCE ) << "Total update time: "<<t_lat<<" microsecs.";
    } else {
      edm::LogInfo( MSGSOURCE ) << "Nothing to do: payloads "<<payloads.size()<<" targetTime "<<targetTime<<" lastSince "<<lastSince;
    }
  }
 
}

#endif //  POPCON_POPCONFORLUMITOHLT_H


