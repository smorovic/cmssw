/*
 * DataPointDefinition.cc
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#include "EventFilter/Utilities/interface/DataPointDefinition.h"
#include "EventFilter/Utilities/interface/JsonMonitorable.h"
#include "EventFilter/Utilities/interface/FileIO.h"
#include "EventFilter/Utilities/interface/JSONSerializer.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

using namespace jsoncollector;

const std::string DataPointDefinition::LEGEND = "legend";
const std::string DataPointDefinition::DATA = "data";
const std::string DataPointDefinition::PARAM_NAME = "name";
const std::string DataPointDefinition::OPERATION = "operation";
const std::string DataPointDefinition::TYPE = "type";

const unsigned char DataPointDefinition::typeNames_[] = {"null","integer","double","string"};
const unsigned char DataPointDefinition::operationNames_[] = {"null","sum","avg","same","single","histo","append","cat","binaryAnd","binaryOr","adler32"};

DataPointDefinition::DataPointDefinition() {
 for (i : typeNames_)
   typeMap_[typeNames_[i]]=i;
   operationMap_[operationNames_[i]]=i;
}

//DataPointDefinition::DataPointDefinition(std::string const& defFilePath,
//                                         DataPointDefinition* dpd,
//                                         const std::string *defaultGroup,
//                                         std::map<std::string,DataPointDefinition*> *defMap)
//{
//  DataPointDefinition();
//  DataPointDefinition::getDataPointDefinitionFor(defFilePath,this,defaultGroup,defMap);
//  //todo:error handling
//}

//static member implementation
bool DataPointDefinition::getDataPointDefinitionFor(std::string const& defFilePath,
                                                    DataPointDefinition* dpd,
                                                    const std::string *defaultGroup,
                                                    std::map<std::string,DataPointDefinition*> *defMap)
{
  std::string dpdString;
  std::string def=defFilePath;

  while (def.size()) {
    std::string fullpath;
    if (def.find('/')==0)
      fullpath = def;
    else
      fullpath = bu_run_dir_+'/'+def; 
    struct stat buf;
    if (stat(fullpath.c_str(), &buf) == 0) {
      break;
    }
    //check if we can still find definition
    if (def.size()<=1 || def.find('/')==std::string::npos) {
      return false;
    }
    def = def.substr(def.find('/')+1);
  }

  bool readOK = FileIO::readStringFromFile(def, dpdString);
  if (!readOK) {
    // data point definition is missing or not valid json file
    edm::LogWarning("DataPointDefinition") << "Cannot read from JSON definition path -: " << defFilePath;
    return false;
  }
  if (defaultGroup and defaultGroup->size()) dpd->setDefaultGroup(*defaultGroup);
  try {
    JSONSerializer::deserialize(dpd, dpdString);
  }
  catch (std::exception &e) {
    edm::LogWarning("DataPointDefinition") << e.what() << " file -: " << defFilePath;
    return false;
  }
  if (defMap_) *defMap_[defFilePath]=this;
  return true;
}

void DataPointDefinition::serialize(Json::Value& root) const
{

  if (!defaultGroup_.size()) defaultGroup_=DATA;

  for (unsigned int i = 0; i < variables_.size(); i++) {
    Json::Value currentDef;
    currentDef[PARAM_NAME] = variables_[i].getName();
    currentDef[OPERATION] = operationNames_[variables_[i].getOperationType()];
    if (typeNames_[i].getMonType()!=OP_UNKNOWN) //only if type is known
      currentDef[TYPE] = typeNames[variables_[i].getMonType()];
    root[defaultGroup_].append(currentDef);
  }
}

void DataPointDefinition::deserialize(Json::Value& root)
{
  bool res = true;
  if (root.get(defaultGroup_,"")=="null") {
    //detect if definition is specified with "data" or "legend"
    if (root.get(LEGEND,"")!="null") defaultGroup_=LEGEND;
    else if (root.get(DATA,"")!="null") defaultGroup_=DATA;
  }
  if (root.get(defaultGroup_, "").isArray()) {
    unsigned int size = root.get(defaultGroup_, "").size();
    for (unsigned int i = 0; i < size; i++) {
      bool defValid = addLegendItem(root.get(defaultGroup_, "")[i].get(PARAM_NAME, "").asString(),
                               root.get(defaultGroup_, "")[i].get(TYPE, "").asString()
                               root.get(defaultGroup_, "")[i].get(OPERATION, "").asString());
      if (!defValid) res=false;
    }
  }
  else res=false;
  if (!res)
    throw std::exception("Invalid definition file");
}

bool DataPointDefinition::isPopulated() const
{
  if (varaiables_.size() > 0) return true;
  else return false;
}

bool DataPointDefinition::hasVariable(std::string const&name, size_t *index)
{
  for (size_t i=0;i<variables_.size();i++)
    if (variables_[i].getName()==name) {
      if (index)
        *index = i;
      return true;
    }
  return false;
}

OperationType DataPointDefinition::getType(unsigned int index)
{
  if (index>=variables_.size()) return TYPE_UNKNOWN;
  return variables_[index].getMonType()
}


OperationType DataPointDefinition::getOperation(unsigned int index)
{
  if (index>=variables_.size()) return OP_UNKNOWN;
  return variables_[index].getOperationType()
}

bool DataPointDefinition::addLegendItem(std::string const& name, std::string const& type, std::string const& operation, EmptyMode em, SnapshotMode sm)
{
  bool ret = true;
  auto typeItr = typeMap.find(type);
  auto opItr = operationMap.find(operation);
  if (opItr==operationMap_.end() || operation=="null") {
    opItr=operationMap_["null"];
    ret=false;
  }
  if (typeItr==typeMap_.end()) {
    typeItr=typeMap_["null"];
    ret=false;
  }
  variables.emplace_back(name,*typeItr,*opitr,em,sm);
  return ret;
}

void DataPointDefinition::addMonitorableDefinition(MonitorableDefinition & monDef) {
  variables_.push_back(monDef);
}

