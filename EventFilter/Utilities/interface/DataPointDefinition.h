/*
 * DataPointDefinition.h
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#ifndef DATAPOINTDEFINITION_H_
#define DATAPOINTDEFINITION_H_

#include "EventFilter/Utilities/interface/JsonMonitorable.h"

namespace jsoncollector {

class MonitorableDefinition {
  public:
    MonitorableDefinition(std::string const& name, MonType type, OperationType op, bool isValidated=false, EmptyMode em=EM_UNSET, SnapshotMode sm=SM_UNSET):
      name_(name),
      type_(type),
      op_(op),
      em_(em),
      sm_(sm),
      isValidated_(isValidated)
      {}

    std::string const& getName() const {return name_;}
    void setName(std::string const& name) {name_=name;}

    MonType getType() const {return type_;}
    void setType(MonType type) {type_=type;}

    OperationType getOperation() const {return op_;}
    void setOperation(OperationType op) {op_=op;}

    EmptyMode getEmptyMode() const {return em_;}
    void setEmptyMode(EmptyMode em) {em_=em;}

    EmptyMode getSnapshotMode() const {return sm_;}
    void setSnapshotMode(SnapshotMode sm) {sm_=sm;}

    bool isValidated() {return isValidated;}

  private:
    std::string name_;
    MonType type_;
    OperationType op_;
    EmptyMode em_;
    SnapshotMode sm_;
    bool isValidated_;
    
};




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
  std::vector<MonitorableDefinition> const& getVariables() {return variables_;}
  std::vector<std::string> const& getOperations() {return opNames_;}

  /**
   * Loads a DataPointDefinition from a specified reference
   */
  static bool getDataPointDefinitionFor(std::string const& defFilePath,
                                        DataPointDefinition* dpd, 
                                        const std::string *defaultGroup=nullptr,
                                        const std::map<std::string,DataPointDefinition*> defMap=nullptr);

  void setDefaultGroup(std::string const& group) {defaultGroup_=group;}

  void addLegendItem(std::string const& name, std::string const& type, std::string const& operation, bool isValidated, EmptyMode em = EM_UNDEF, SnapshotMode sm = SM_UNDEF);

  void addMonitorableDefinition(MonitorableDefinition & monDef);

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

private:
  std::vector<MonitorableDefinition> variables_;
  std::string defFilePath_;
  std::string defaultGroup_;

};

}

#endif /* DATAPOINTDEFINITION_H_ */
