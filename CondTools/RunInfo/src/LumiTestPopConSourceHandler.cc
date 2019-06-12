#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "CondCore/CondDB/interface/ConnectionPool.h"
#include "CondFormats/Common/interface/TimeConversions.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "CondTools/RunInfo/interface/LumiTestPopConSourceHandler.h"
#include "CondCore/CondDB/interface/DecodingKey.h"

LumiTestPopConSourceHandler::LumiTestPopConSourceHandler( edm::ParameterSet const & pset ):
  m_name( pset.getUntrackedParameter<std::string>( "name", "LumiTestPopConSourceHandler" ) ),
  m_maxDataSize( pset.getUntrackedParameter<unsigned int>( "maxDataSize", 1000000000 ) ){
}

LumiTestPopConSourceHandler::~LumiTestPopConSourceHandler() {}

void LumiTestPopConSourceHandler::getNewObjects() {
  cond::LumiTestPayload* thePayload = new cond::LumiTestPayload;
  cond::auth::KeyGenerator gen;
  thePayload->m_data = gen.make( m_maxDataSize );
  boost::posix_time::ptime creationTime = boost::posix_time::second_clock::local_time();
  thePayload->m_creationTime = cond::time::from_boost( creationTime );  
  appendToTransfer( thePayload, 1 );
}

std::string LumiTestPopConSourceHandler::id() const { 
  return m_name;
}
