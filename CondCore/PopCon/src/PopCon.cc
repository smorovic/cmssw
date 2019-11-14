#include "CondCore/PopCon/interface/PopCon.h"
#include "CondCore/PopCon/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include <iostream>

namespace popcon {

  constexpr const char* const PopConBase::s_version;

  PopConBase::PopConBase(const edm::ParameterSet& pset):
    m_targetSession(),
    //m_targetConnectionString(pset.getUntrackedParameter< std::string >("targetDBConnectionString","")),
    m_authPath( pset.getUntrackedParameter<std::string>("authenticationPath","")),
    m_authSys( pset.getUntrackedParameter<int>("authenticationSystem",1)),
    m_record(pset.getParameter<std::string> ("record")),
    m_payload_name(pset.getUntrackedParameter<std::string> ("name","")),
    m_LoggingOn(pset.getUntrackedParameter< bool > ("loggingOn",true)),
    m_endOfValidity(pset.getUntrackedParameter< cond::Time_t > ("lastTill",0))
    {
      //TODO set the policy (cfg or global configuration?)
      //Policy if corrupted data found
      
      edm::LogInfo ("PopCon") << "This is PopCon (Populator of Condition) v" << s_version << ".\n"
                              << "Please report any problem and feature request through the JIRA project CMSCONDDB.\n" ; 
    }
  
  PopConBase::~PopConBase(){
    /**
    if( !m_targetConnectionString.empty() )  {
      m_targetSession.transaction().commit();
    }
    **/
  }
 
  cond::persistency::Session PopConBase::initialize() {	
    edm::LogInfo ("PopCon")<<"payload name "<<m_payload_name<<std::endl;
    if(!m_dbService.isAvailable() ) throw Exception("DBService not available");
    const std::string & connectionStr = m_dbService->session().connectionString();
    m_tagInfo.name = m_dbService->tag(m_record);
    m_targetSession = m_dbService->session();
    /**
    if( m_targetConnectionString.empty() ) m_targetSession = m_dbService->session();
    else {
      cond::persistency::ConnectionPool connPool;
      connPool.setAuthenticationPath( m_authPath );
      connPool.setAuthenticationSystem( m_authSys );
      connPool.configure();
      m_targetSession = connPool.createSession( m_targetConnectionString );
      m_targetSession.transaction().start();
    }
    **/
    if( m_targetSession.existsIov( m_tagInfo.name ) ){
      cond::persistency::IOVProxy iov = m_targetSession.readIov( m_tagInfo.name );
      //m_tagInfo.name = m_tag;
      m_tagInfo.lastInterval = iov.getLast();

      edm::LogInfo ("PopCon") << "destination DB: " << connectionStr
      //                              << ", target DB: " << ( m_targetConnectionString.empty() ? connectionStr : m_targetConnectionString ) << "\n"
                              << "TAG: " << m_tagInfo.name
                              << ", last since/till: " <<  m_tagInfo.lastInterval.since
                              << "/" << m_tagInfo.lastInterval.till << std::endl;
    } else {
      edm::LogInfo ("PopCon") << "destination DB: " << connectionStr
	//                              << ", target DB: " << ( m_targetConnectionString.empty() ? connectionStr : m_targetConnectionString ) << "\n"
                              << "TAG: " << m_tagInfo.name
                              << "; First writer to this new tag." << std::endl;
    }
    return m_targetSession;
  }

  void PopConBase::insertIov( cond::Hash payloadId, cond::Time_t since ){
    m_dbService->appendSinceTime( payloadId, since, m_record, m_LoggingOn );
  }

  void PopConBase::eraseIov( cond::Hash payloadId, cond::Time_t since ){
    m_dbService->eraseSinceTime( payloadId, since, m_record, m_LoggingOn );
  }

  const cond::TagInfo_t& PopConBase::tagInfo(){
    return m_tagInfo;
  }
   
  const cond::LogDBEntry_t& PopConBase::logDBEntry(){
    return m_logDBEntry;
  }

  bool PopConBase::isLoggingOn(){
    return m_LoggingOn;
  }

  void PopConBase::setLogHeader(const std::string& sourceId, const std::string& message){
    m_dbService->setLogHeaderForRecord(m_record, sourceId, message);
  }
  
  void PopConBase::finalize() {
    /**
    if( !m_targetConnectionString.empty() )  {
      if (m_endOfValidity) {
	m_dbService->closeIOV(m_endOfValidity,m_record);
      }
      m_targetSession.transaction().commit();
    }
    **/
    if (m_endOfValidity) {
      m_dbService->closeIOV(m_endOfValidity,m_record);
    }
    m_dbService->postEndJob();
  }

  PopCon::PopCon(const edm::ParameterSet& pset):
    PopConBase( pset ){
  }

  PopCon::~PopCon(){
  }
  
}
