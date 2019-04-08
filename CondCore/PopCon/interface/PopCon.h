#ifndef POPCON_POPCON_H
#define POPCON_POPCON_H
//
// Author: Vincenzo Innocente
// Original Author:  Marcin BOGUSZ
// 


#include "CondCore/DBOutputService/interface/PoolDBOutputService.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/ParameterSet/interface/ParameterSetfwd.h"

#include "CondCore/CondDB/interface/Time.h"

#include <boost/bind.hpp>
#include <algorithm>
#include <vector>
#include <string>


#include<iostream>


namespace popcon {

  /* Base populator of the Condition DB
   *
   */
  class PopConBase {
  public:

    PopConBase(const edm::ParameterSet& pset);
     
    virtual ~PopConBase();

    void setLogHeader(const std::string& sourceId, const std::string& message);
    
    virtual void finalize();
    const cond::TagInfo_t& tagInfo();
    const cond::LogDBEntry_t& logDBEntry();
    bool isLoggingOn();
     
  protected:
    cond::persistency::Session initialize();
    template<typename T>
    cond::Hash writeOne(T * payload, cond::Time_t time);
    void insertIov( cond::Hash payloadId, cond::Time_t since );
    void eraseIov( cond::Hash payloadId, cond::Time_t since );

  private:

    edm::Service<cond::service::PoolDBOutputService> m_dbService;

    cond::persistency::Session m_targetSession;

    //std::string m_targetConnectionString;

    std::string m_authPath;

    int m_authSys;
    
    std::string  m_record;
    
    std::string m_payload_name;
    
    bool m_LoggingOn;

    //std::string m_tag;
    
    cond::TagInfo_t m_tagInfo;
    
    cond::LogDBEntry_t m_logDBEntry;

    cond::Time_t m_endOfValidity;

    static constexpr const char* const s_version = "6.0";
  };


  template<typename T>
  cond::Hash PopConBase::writeOne(T * payload, cond::Time_t time) {
    return m_dbService->writeOne(payload, time, m_record, m_LoggingOn);
  }

  
  template<typename Container>
  void displayHelper(Container const & payloads) {
    typename Container::const_iterator it;
    for (it = payloads.begin(); it != payloads.end(); it++)
      edm::LogInfo ("PopCon")<< "Since " << (*it).second << std::endl;
  }     
  
  
  template<typename Container>
  const std::string displayIovHelper(Container const & payloads) {
    if (payloads.empty()) return std::string("Nothing to transfer;");
    std::ostringstream s;
    // when only 1 payload is transferred; 
    if ( payloads.size()==1)
      s << "Since " << (*payloads.begin()).second <<  "; " ;
    else{
      // when more than one payload are transferred;  
      s << "first payload Since " <<  (*payloads.begin()).second <<  ", "
        << "last payload Since "  << (*payloads.rbegin()).second <<  "; " ;  
    }  
    return s.str();
  }
  
  class PopCon : public PopConBase {
  public:
    PopCon(const edm::ParameterSet& pset);
     
    virtual ~PopCon();

    template<typename Source>
    void write(Source const & source);

  }; 
  
  template<typename Source>
  void PopCon::write(Source const & source) {
    typedef typename Source::value_type value_type;
    typedef typename Source::Container Container;
    
    std::pair<Container const *, std::string const> ret = source(initialize(),
  								 tagInfo(),logDBEntry()); 
    Container const & payloads = *ret.first;
    
    if(PopConBase::isLoggingOn()) {
      std::ostringstream s;
      s << "PopCon v" << s_version << "; " << displayIovHelper(payloads) << ret.second;
      //s << "PopCon v" << s_version << "; " << cond::userInfo() << displayIovHelper(payloads) << ret.second;
      PopConBase::setLogHeader( source.id(),s.str() );
    }
    displayHelper(payloads);
    
    std::for_each(payloads.begin(),payloads.end(),
		  boost::bind(&popcon::PopConBase::writeOne<value_type>,this,
			      boost::bind(&Container::value_type::first,_1),
			      boost::bind(&Container::value_type::second,_1)
			      )
		   );
    
    
    finalize();
  }
 
}

#endif //  POPCON_POPCON_H


