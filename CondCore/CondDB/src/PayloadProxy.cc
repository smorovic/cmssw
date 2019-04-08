#include "CondCore/CondDB/interface/PayloadProxy.h"

namespace cond {

  namespace persistency {

    BasePayloadProxy::BasePayloadProxy() :
      m_iovProxy(),m_session() {
    }

    BasePayloadProxy::~BasePayloadProxy(){}

    void BasePayloadProxy::setUp( Session dbSession ){
      m_session = dbSession;
      invalidateCache();    
    }
    
    void BasePayloadProxy::loadTag( const std::string& tag ){
      m_session.transaction().start(true);
      m_iovProxy = m_session.readIov( tag );
      m_session.transaction().commit();
      invalidateCache();
    }
    
    void BasePayloadProxy::loadTag( const std::string& tag, const boost::posix_time::ptime& snapshotTime ){
      m_session.transaction().start(true);
      m_iovProxy = m_session.readIov( tag, snapshotTime );
      m_session.transaction().commit();
      invalidateCache();
    }

    void BasePayloadProxy::reload(){
      std::string tag = m_iovProxy.tagInfo().name;
      if( !tag.empty() ) loadTag( tag );
    }
    
    ValidityInterval BasePayloadProxy::setIntervalFor(cond::Time_t target, bool load) {
      if( !m_currentIov.isValidFor( target ) ){
	m_currentIov.clear();
	m_session.transaction().start(true);
	m_currentIov = m_iovProxy.getInterval( target );
        if(load) loadPayload();
	m_session.transaction().commit();
      }
      return ValidityInterval( m_currentIov.since, m_currentIov.till );
    }

    ValidityInterval BasePayloadProxy::setIntervalFor( Time_t target, Time_t defaultIovSize ){
      if( !m_currentIov.isValidFor( target ) ){
	m_currentIov.clear();
	m_session.transaction().start(true);
	m_currentIov = m_iovProxy.getInterval( target, defaultIovSize );
	m_session.transaction().commit();
      }
      return ValidityInterval( m_currentIov.since, m_currentIov.till );
    }
    
    bool BasePayloadProxy::isValid() const {
      return m_currentIov.isValid();
    }

    IOVProxy BasePayloadProxy::iov() {
      return m_iovProxy;
    }

  }
}
