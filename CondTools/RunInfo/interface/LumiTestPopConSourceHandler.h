#ifndef LUMITESTPOPCONSOURCEHANDLER_H
#define LUMITESTPOPCONSOURCEHANDLER_H

#include <string>

#include "CondCore/PopCon/interface/PopConSourceHandler.h"
#include "CondFormats/Common/interface/LumiTestPayload.h"
#include "FWCore/ParameterSet/interface/ParameterSetfwd.h"

class LumiTestPopConSourceHandler : public popcon::PopConSourceHandler<cond::LumiTestPayload>{
 public:
  LumiTestPopConSourceHandler( const edm::ParameterSet& pset ); 
  ~LumiTestPopConSourceHandler() override;
  void getNewObjects() override;
  std::string id() const override;
private:
  std::string m_name; 
  unsigned int m_maxDataSize;
};
  
#endif
