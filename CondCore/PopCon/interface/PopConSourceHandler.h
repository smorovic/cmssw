#ifndef  PopConSourceHandler_H
#define  PopConSourceHandler_H

#include "CondCore/CondDB/interface/Session.h"
#include "CondCore/CondDB/interface/Time.h"

#include <boost/bind.hpp>
#include <algorithm>
#include <memory>
#include <vector>
#include <string>

namespace popcon {

  /** Online DB source handler, aims at returning the vector of data to be 
   * transferred to the online database
   * Subdetector developers inherit over this class with template parameter of 
   * payload class; 
   * need just to implement the getNewObjects method that loads the calibs,
   * the sourceId methods that return a text identifier of the source,
   * and provide a constructor that accept a ParameterSet
   */
  template <class T>
  class PopConSourceHandler{
  public: 
    typedef T value_type;
    typedef PopConSourceHandler<T> self;
    typedef std::vector<std::pair<value_type*, cond::Time_t> > Container;
    
    class Ref {
    public:
      Ref() : m_dbsession(){}
      Ref(cond::persistency::Session& dbsession, const std::string& hash) : 
        m_dbsession(dbsession){
	m_d = m_dbsession.fetchPayload<T>( hash );
      }
      ~Ref() {
      }
      
      Ref(const Ref & ref) :
        m_dbsession(ref.m_dbsession), m_d(ref.m_d) {
      }
      
      Ref & operator=(const Ref & ref) {
        m_dbsession = ref.m_dbsession;
        m_d = ref.m_d;
        return *this;
      }
      
      T const * ptr() const {
        return m_d.get();
      }
      
      T const * operator->() const {
        return ptr();
      }
      // dereference operator
      T const & operator*() const {
        return *ptr();
      }
      
      
    private:
      
      cond::persistency::Session m_dbsession;
      std::shared_ptr<T> m_d;
    };
    
    
    PopConSourceHandler():
      m_tagInfo(nullptr),
      m_logDBEntry(nullptr)
    {}
    
    virtual ~PopConSourceHandler(){
    }
    
    
    cond::TagInfo_t const & tagInfo() const { return  *m_tagInfo; }
    
    // return last paylod of the tag
    Ref lastPayload() const {
      return Ref(m_session,tagInfo().lastInterval.payloadId);
    }
    
    // return last successful log entry for the tag in question
    cond::LogDBEntry_t const & logDBEntry() const { return *m_logDBEntry; }
    
    // FIX ME
    void initialize (const cond::persistency::Session& dbSession,
      		     cond::TagInfo_t const & tagInfo, cond::LogDBEntry_t const & logDBEntry) { 
      m_session = dbSession;
      m_tagInfo = &tagInfo;
      m_logDBEntry = &logDBEntry;
    }
    
    // this is the only mandatory interface
    std::pair<Container const *, std::string const>  operator()(const cond::persistency::Session& session,
      							cond::TagInfo_t const & tagInfo, 
      							cond::LogDBEntry_t const & logDBEntry) const {
      const_cast<self*>(this)->initialize(session, tagInfo, logDBEntry);
      return std::pair<Container const *, std::string const>(&(const_cast<self*>(this)->returnData()), userTextLog());
    }
    
    Container const &  returnData() {
      getNewObjects();
      sort();
      return m_to_transfer;
    }
    
    std::string const & userTextLog() const { return m_userTextLog; }
    
    //Implement to fill m_to_transfer vector and  m_userTextLog
    //use getOfflineInfo to get the contents of offline DB
    virtual void getNewObjects()=0;
    
    // return a string identifing the source
    virtual std::string id() const=0;
    
    void sort() {
      std::sort(m_to_transfer.begin(),m_to_transfer.end(),
		boost::bind(std::less<cond::Time_t>(),
			    boost::bind(&Container::value_type::second,_1),
			    boost::bind(&Container::value_type::second,_2)
			    )
		);
    }

    void appendToTransfer( value_type* payload, cond::Time_t since ){
      m_payloads.emplace_back( std::unique_ptr<value_type>( payload ) );
      m_to_transfer.push_back( std::make_pair( payload, since ) );
    }
    
  protected:

    cond::persistency::Session& dbSession() const {
      return m_session;
    }
    
  private:
    
    mutable cond::persistency::Session m_session;
    
    cond::TagInfo_t const * m_tagInfo;
    
    cond::LogDBEntry_t const * m_logDBEntry;

    std::vector<std::unique_ptr<value_type> > m_payloads;

  protected:
    
    //vector of payload objects and iovinfo to be transferred
    //class looses ownership of payload object
    Container m_to_transfer;

  protected:
    std::string m_userTextLog;

  };
}
#endif
