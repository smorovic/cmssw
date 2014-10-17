/*
 * DataPoint.cc
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#include "EventFilter/Utilities/interface/DataPoint.h"

#include <tbb/concurrent_vector.h>

#include <algorithm>
#include <assert.h>

//max collected updates per lumi
#define MAXUPDATES 200
#define MAXBINS

using namespace jsoncollector;

const std::string DataPoint::SOURCE = "source";
const std::string DataPoint::DEFINITION = "definition";
const std::string DataPoint::DATA = "data";

//constructor for taking snapshots of tracked variables
DataPointCollection::DataPointCollection(std::vector<TrackedMonitorable> * trackedVector,bool fastMon): trackedVector_(trackedVector),fastMon_(fastMon)
{
  for (t : *trackedVector)
  {
    //object copy
    data_.push_back(new DataPoint(t,true,fastMon));
  }
}

//constructor for parsing json files containing definition field
DataPointCollection::DataPointCollection(std::string const & filename, std::map<std::string,DataPointDefinition> *defMap)
:defMap_(defMap)
{
  Json::Value root;
  JSONSerializer::deserialize(root,name);
}


//constructor for parsing json file name
DataPointCollection::DataPointCollection(std::string const & definition,std::string const& filename)
:defMap_(defMap)
{
  Json::Value root;
  JSONSerializer::deserialize(root,name);
}


//reset all snapped data
void DataPointCollection::reset() {
  for (dp : data_) dp->reset();
}

DataPointCollection::~DataPointCollection() {
  for (d :data_) delete d;
}

void DataPointCollection::serialize(Json::Value& root) const
{
  if (initSourceMaybe()) {
    root[SOURCE] = source_;
  }
  if (initDefinitionStringMaybe()) {
    root[DEFINITION] = definition_;
  }
  for (unsigned int i=0;i<data_.size();i++)
    root[DATA].append(data_[i]->getValue());
}

void DataPointCollection::deserialize(Json::Value& root)
{
  source_ = root.get(SOURCE, "").asString();
  definition_ = root.get(DEFINITION, "").asString();
  MonType type = TYPE_UNKNOWN;
  OpType op = OP_UNKNOWN;
  if (defMap_ && !def_) {
    auto itr = defMap_->find(definition_);
    if (itr == defMap_->end()) {
      auto dpd = new DataPointDefinition();
      if (DataPointDefinition::getDataPointDefinitionFor(definition_,dpd,nullptr,defMap_))
        def_=dpd;
      else delete dpd;
    }
    else def_=*itr;
  }

  if (root.get(DATA, "").isArray()) {
    unsigned int size = root.get(DATA, "").size();
    for (unsigned int i = 0; i < size; i++)
    {
      if (def_) {
        data_.push_back(new DataPoint(root.get(DATA, "")[i],def_->getMonitorableDefinition(i)));
      }
      else
        //still allow to deserialize without valid definition
        if (typeAndOperationCheck(type,op)) return;//TODO:throw exception?
        data_.push_back(new DataPoint(root.get(DATA, "")[i],MonitorableDefinition md(type,op,false)));
    }
  }
}

void DataPointCollection::snap()
{
  for (d:data_) d->snapTimer();
}

void DataPointCollection::snapTimer()
{
  for (d:data_) d->snapTimer();
}

void DataPointCollection::snapGlobalEOL()
{
  for (d:data_) d->snapGlobalEoL();
}

void DataPointCollection::snapStreamEoL(unsigned int streamID)
{
   for (d:data_) d->snapStreamEoL(streamID);
}

JsonMonitorable* DataPointCollection::mergeAndRetrieveMonitorable(std::string const& name)
{
  size_t i;
  if (!def_.getIndex(name,i)) return nullptr;//todo
  return data_[i]->mergeAndRetrieveMonitorable();
}

JsonMonitorable* DataPointCollection::mergeAndRetrieveMonitorable(size_t index)
{
  if (index>=data_.size()) return nullptr;
  //assume the caller takes care of deleting the object
  return data_[index]->mergeAndRetrieveMonitorable();
}

Json::Value && DataPointCollection::mergeAndSerialize()
{
  Json::Value root
  root[SOURCE] = source_;
  root[DEFINITION] = definition_;
  for (d : data_) {
      root[DATA].append(d->getJsonValue());
  }
  return root;
}

bool DataPointCollection::mergeCollections(std::vector<DataPointCollection*>& collections)
{
  //assert(nStreams_==1);//function is allowed only for this case
  if (!data_.size()) {
    assert(!collections.size());
    def_ = collections[0]->getDef();
    for (size_t i=0;i<collections[0].size()) {
      data.push_back(new DataPoint(collections[0].at(i)->getTracked(),collections[0].at(i)->getMonitorable()));
    }
  }
  std::vector<JsonMonitorable*> dpArray(collections.size());
  for (size_t i=0;i<data_.size();i++) {
    //build array..
    for (size_t j=0;j<collections.size();j++) {
      dpArray[j] = collections[j]->getDataAt(i).getMonitorable();
      if (dpArray[j]==nullptr) return false;
    }
    data_[i].mergeAndSerializeMonitorables(monArray);
  }
  return true;
  //run mergeAndSerialize to get output
}

bool DataPointCollection::mergeTrackedCollections(std::vector<DataPointCollection*>& collections)
{
  //assert(nStreams_==1);//function is allowed only for this case
  if (!data_.size()) {
    assert(!collections.size());
    def_ = collections[0]->getDef();
    for (size_t i=0;i<collections[0].size()) {
      data.push_back(new DataPoint(collections[0].at(i)->getTracked(),collections[0].at(i)->getMonitorable()));
    }
  }
  std::vector<JsonMonitorable*> dpArray(collections.size());
  for (size_t i=0;i<data_.size();i++) {
    //build array..
    for (size_t j=0;j<collections.size();j++) {
      auto mvec = collections[j]->getDataAt(i).getTracked().mon_;
      if (mvec || mvec->size()) return false;
      dpArray[j] = mvec_->at(0);
      if (dpArray[j]==nullptr) return false;
    }
    data_[i]->mergeMonitorables(monArray);
  }
  return true;
  //run mergeAndSerialize to get output
}
