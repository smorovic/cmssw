#ifndef Cond_LumiTestPayload_h
#define Cond_LumiTestPayload_h

#include "CondFormats/Serialization/interface/Serializable.h"
#include <iostream>

namespace cond {
  
  /** Test class for condition payload
  */
  class LumiTestPayload {
  public:
    LumiTestPayload():m_id(0),m_data(""){
    }
    virtual ~LumiTestPayload(){
    }

  public:
    unsigned long long m_id;
    unsigned long long m_creationTime; 
    std::string m_data;
  
  COND_SERIALIZABLE;
};
  
}

#endif
