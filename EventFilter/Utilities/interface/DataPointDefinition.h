/*
 * DataPointDefinition.h
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#ifndef DATAPOINTDEFINITION_H_
#define DATAPOINTDEFINITION_H_

#include "EventFilter/Utilities/interface/JsonMonitorable.h"
#include "EventFilter/Utilities/interface/JsonSerializable.h"
#include <string>
#include <vector>

namespace jsoncollector {

class JsonMonConfig;

class DataPointDefinition: public JsonSerializable {

public:
  DataPointDefinition();
  //DataPointDefinition(std::string const& defFilePath,
  //                    DataPointDefinition* dpd,
  //                    const std::string *defaultGroup=nullptr,
  //                    const std::map<std::string,DataPointDefinition*> defMap=nullptr);

  virtual ~DataPointDefinition() {}

  /**
   * JSON serialization procedure for this class
   */
  virtual void serialize(Json::Value& root) const;
  /**
   * JSON deserialization procedure for this class
   */
  virtual void deserialize(Json::Value& root);
  /**
   * Returns true if the legend_ has elements
   */
  bool isPopulated() const;
  /**
   * Returns a LegendItem object ref at the specified index
   */
  std::vector<std::string> const& getNames() {return varNames_;}
  std::vector<std::string> const& getOperations() {return opNames_;}

  /**
   * Loads a DataPointDefinition from a specified reference
   */
  static bool getDataPointDefinitionFor(std::string const& defFilePath,
                                        DataPointDefinition* dpd, 
                                        const std::string *defaultGroup=nullptr,
                                        const std::map<std::string,DataPointDefinition*> defMap=nullptr);

  void setDefaultGroup(std::string const& group) {defaultGroup_=group;}

  void addLegendItem(std::string const& name, std::string const& type, std::string const& operation);

  bool hasVariable(std::string const&name,size_t *index=nullptr);
  OperationType getOperationFor(unsigned int index);

  std::string & getDefFilePath() {return defFilePath_;}

  //known JSON type names
  static const unsigned char typeNames_[];
  static const unsigned char operationNames_[];

  std::map<std::string,MonType> typeMap_;
  std::map<std::string,OperationType> operationMap_;

  // JSON field names
  static const std::string LEGEND;
  static const std::string DATA;
  static const std::string PARAM_NAME;
  static const std::string OPERATION;
  static const std::string TYPE;

  class MonitorableDefinition {
    public:
      MonitorableDefinition(std::string name,MonType monType, OperationType opType):name_(name),monType_(monType),opType(opType) {}

      std::string const& getName() const {return name_;}
      void setName(std::string const& name) {name_=name;}

      MonType getMonType() const {return monType_;}
      void setMonType(MonType monType) {monType_=monType;}

      OperationType getOperation() const {return opType_;}
      void setOperation(OperationType opType) {opType_=opType;}
    private:
      std::string name_;
      MonType monType_;
      OperationType opType_;
      bool isVector_;
    
  };

private:
  std::vector<MonitorableDefinition> variables_;
  std::string defFilePath_;
  std::string defaultGroup_;

};
}

#endif /* DATAPOINTDEFINITION_H_ */
