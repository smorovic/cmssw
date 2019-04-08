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
    explicit LumiTestPayload( unsigned long long t ):m_id(t),m_data(""){
    }
    virtual ~LumiTestPayload(){}

  public:
    unsigned long long m_id;
    std::string m_data;
  
  COND_SERIALIZABLE;
};
  
}

#endif
