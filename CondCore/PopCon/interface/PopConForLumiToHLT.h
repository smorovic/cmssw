#ifndef POPCON_POPCONFORLUMITOHLT_H
#define POPCON_POPCONFORLUMITOHLT_H
//
#include "CondCore/PopCon/interface/PopCon.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

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
    cond::Time_t preLoadConditions( cond::Time_t targetTime );

  private:
    static constexpr const char* const MSGSOURCE = "PopConForLumiToHLT";
    size_t m_latencyInLumisections;
    std::string m_preLoadConnectionString;
    std::string m_pathForLastLumiFile;
    bool m_debug;
  };

  template<typename Source>
  void PopConForLumiToHLT::write(Source const & source) {
    typedef typename Source::value_type PayloadType;
    typedef typename Source::Container Container;
    
    std::pair<Container const *, std::string const> ret = source(initialize(),
  								 tagInfo(),logDBEntry()); 
    Container const & payloads = *ret.first;
    
    if(PopConBase::isLoggingOn()) {
      std::ostringstream s;
      s << "PopCon v" << s_version << "; " << displayIovHelper(payloads) << ret.second;
      PopConBase::setLogHeader( source.id(),s.str() );
    }
    displayHelper(payloads);

    cond::Time_t targetTime = getLastLumiProcessed()+m_latencyInLumisections;
    edm::LogInfo( MSGSOURCE ) << "Found "<< payloads.size() << " payload(s) in the queue.";
    edm::LogInfo( MSGSOURCE ) << "Last since: "<<PopConBase::tagInfo().lastInterval.since<<" target since: "<<targetTime;
    if( payloads.size() > 0 &&
	// this check is not required if the policy update is kept as the general for synch=offline, express, primpt, hlt, mc
	targetTime > PopConBase::tagInfo().lastInterval.since ){
      auto p = payloads.back();
      // insert the new lumisection...
      /** remove me - this is only for the cond::LumiTestPayload class **/
      const_cast<PayloadType*>(p.first)->m_id = targetTime; 
      /**                                                             **/
      cond::Hash payloadId = PopConBase::writeOne<PayloadType>( p.first, targetTime );
      PopConBase::finalize();
      // check for late updates...
      cond::Time_t lastProcessed = getLastLumiProcessed();
      edm::LogInfo( MSGSOURCE ) << "Last lumisection processed after update: "<<lastProcessed;
      // check the pre-loaded iov
      edm::LogInfo( MSGSOURCE ) << "Preloading lumisection "<<targetTime;
      cond::Time_t sinceTime = preLoadConditions( targetTime );
      edm::LogInfo( MSGSOURCE ) << "Since time found for target lumi ("<<targetTime<<"): "<<sinceTime;
      if( sinceTime < targetTime ){ 
	// delete the new iov...
	PopConBase::eraseIov( payloadId, targetTime ); 
        // for the moment, only detect the problem...
	edm::LogWarning( MSGSOURCE ) << "Found a late update. History might be superseeded.";
	PopConBase::finalize();
      } 
    }
  }
 
}

#endif //  POPCON_POPCONFORLUMITOHLT_H


