#include "FWCore/Sources/interface/ProducerSourceBase.h"
#include "CondCore/CondDB/interface/Time.h"
#include "FWCore/ParameterSet/interface/ConfigurationDescriptions.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"
#include <string>
namespace cond {
  class EmptyIOVSource : public edm::ProducerSourceBase {
  public:
    EmptyIOVSource(edm::ParameterSet const&, edm::InputSourceDescription const&);
    ~EmptyIOVSource() override;
    static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);
  private:
    void produce(edm::Event & e) override;
    bool setRunAndEventInfo(edm::EventID& id, edm::TimeValue_t& time, edm::EventAuxiliary::ExperimentType& eType) override;
    void initialize(edm::EventID& id, edm::TimeValue_t& time, edm::TimeValue_t& interval) override;
  private:
    TimeType m_timeType;
    Time_t m_firstValid;
    Time_t m_lastValid;
    unsigned int m_interval;
    Time_t m_current;
    unsigned long long m_eventId;
    unsigned int m_maxLumiInRun;
    unsigned int m_maxEvents;
  };
}

#include "FWCore/Utilities/interface/EDMException.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
//#include "DataFormats/Provenance/interface/EventID.h"
//#include <iostream>
namespace cond{
  //allowed parameters: firstRun, firstTime, lastRun, lastTime, 
  //common paras: timetype,interval
  EmptyIOVSource::EmptyIOVSource(edm::ParameterSet const& pset,
				 edm::InputSourceDescription const& desc):
    edm::ProducerSourceBase(pset,desc,true),
    m_timeType( time::timeTypeFromName( pset.getParameter<std::string>("timetype"))),
    m_firstValid( 0 ),
    m_lastValid( 0 ),
    m_interval(  pset.getParameter<unsigned int>("interval") ),
    m_current( 0 ),
    m_eventId( 0 ),
    m_maxLumiInRun( pset.getParameter<unsigned int>("maxLumiInRun") ),
    m_maxEvents( pset.getParameter<unsigned int>("maxEvents" ) ){
    if(m_timeType==cond::runnumber || m_timeType==cond::lumiid ){
      unsigned int firstRun = pset.getParameter<unsigned int>("firstRunnumber");
      unsigned int lastRun = pset.getParameter<unsigned int>("lastRunnumber");
      m_firstValid = firstRun;
      m_lastValid = lastRun;
      if( m_timeType==cond::lumiid ){
	unsigned int firstLumi = pset.getParameter<unsigned int>("firstLumi");
	unsigned int lastLumi = pset.getParameter<unsigned int>("lastLumi");
	m_firstValid = ( m_firstValid << 32 )+ firstLumi;
	m_lastValid = ( m_lastValid << 32 ) + lastLumi;
      }
    } else if ( m_timeType==cond::timestamp ){
      std::string startTime = pset.getParameter<std::string>("startTime");
      if( startTime.size() > 0 ){
	boost::posix_time::ptime bst = boost::posix_time::time_from_string( startTime );
        m_firstValid = cond::time::from_boost( bst );
      }
      std::string endTime = pset.getUntrackedParameter<std::string>("endTime","");
      if( endTime.size() > 0 ){
	boost::posix_time::ptime bet = boost::posix_time::time_from_string( endTime );
	m_lastValid = cond::time::from_boost( bet );
      } 
    }
    m_current=m_firstValid;
  }

  EmptyIOVSource::~EmptyIOVSource() {
  }

  void EmptyIOVSource::produce( edm::Event & ) {
  }  

  bool EmptyIOVSource::setRunAndEventInfo(edm::EventID& id, edm::TimeValue_t& time, edm::EventAuxiliary::ExperimentType& eType){
    m_eventId += 1;
    bool ok = false;
    if( m_eventId <= m_maxEvents ){
      if( m_timeType == cond::runnumber ){
        m_current += m_interval;
        if( m_current <= m_lastValid ) ok = true; 
	if(ok) id = edm::EventID(m_current, id.luminosityBlock(), m_eventId);
      } else if( m_timeType == cond::timestamp ){
        boost::posix_time::ptime next_time = cond::time::to_boost( m_current )+ boost::posix_time::seconds(m_interval);
        m_current = cond::time::from_boost( next_time );
        if( m_current <= m_lastValid ) ok = true; 
        if(ok) {
	  time = m_current;
	  id = edm::EventID(100, 1, m_eventId);
	}
      } else if( m_timeType == cond::lumiid ){
	edm::LuminosityBlockID l(m_current);
        unsigned int lumiId = l.luminosityBlock();
        unsigned int runId = l.run();
        lumiId++;
        if( lumiId >= m_maxLumiInRun ){
	  runId++;
	  lumiId = 1;
	}
        m_current = ( (Time_t)runId << 32 ) + lumiId;
        if( m_current <= m_lastValid ) ok = true; 
        if(ok) id = edm::EventID(runId, lumiId, m_eventId);
      }
    }
    return ok;
  }

  void EmptyIOVSource::initialize(edm::EventID& id, edm::TimeValue_t& time, edm::TimeValue_t& interval){
    if( m_timeType== cond::runnumber){
      id = edm::EventID(m_firstValid, id.luminosityBlock(), 1);
      interval = 0LL; 
    }else if( m_timeType == cond::timestamp ){
      time = m_firstValid;
      interval = m_interval; 
    }else if( m_timeType == cond::lumiid ){
      edm::LuminosityBlockID l(m_firstValid);
      id = edm::EventID(l.run(), l.luminosityBlock(), 1);
      interval = 0LL; 
    }
    m_eventId = 0;
  }

  void
  EmptyIOVSource::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.setComment("Creates runs, lumis and events containing no products.");
    ProducerSourceBase::fillDescription(desc);

    desc.add<std::string>("timetype");
    desc.add<unsigned int>("firstRunnumber")->setComment("The first run number to use");
    desc.add<unsigned int>("lastRunnumber")->setComment("The last run number to use");
    desc.add<unsigned int>("firstLumi")->setComment("The first lumi id to use");
    desc.add<unsigned int>("lastLumi")->setComment("The last lumi id to use");
    desc.add<unsigned int>("maxLumiInRun");
    desc.add<std::string>("startTime");
    desc.add<std::string>("endTime");
    desc.add<unsigned int>("interval");
    desc.add<unsigned int>("maxEvents");

    descriptions.add("source", desc);
  }


}//ns cond

#include "FWCore/Framework/interface/InputSourceMacros.h"
using cond::EmptyIOVSource;

DEFINE_FWK_INPUT_SOURCE(EmptyIOVSource);
