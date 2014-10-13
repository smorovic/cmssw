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

namespace jsoncollector {
  template class HistoJ<unsigned int>;
  template class HistoJ<double>;
  template class VectorJ<unsigned int>;
  template class VectorJ<double>;
}

const std::string DataPoint::SOURCE = "source";
const std::string DataPoint::DEFINITION = "definition";
const std::string DataPoint::DATA = "data";


DataPointCollection::~DataPoint()
{
}

/*
 *
 * Method implementation for simple DataPoint usage
 *
 */

void DataPointCollection::serialize(Json::Value& root) const
{
  if (source_.size()) {
    root[SOURCE] = source_;
  }
  if (definition_.size()) {
    root[DEFINITION] = definition_;
  }
    for (unsigned int i=0;i<data_.size();i++)
      root[DATA].append(data_[i]);
}

void DataPointCollection::deserialize(Json::Value& root)
{
  source_ = root.get(SOURCE, "").asString();
  definition_ = root.get(DEFINITION, "").asString();
  MonType type = TYPE_UNKNOWN;
  OpType op = OP_UNKNOWN;
  if (defMap_) {
    auto itr = defMap_->find(definition_);
    if (itr == defMap_->end()) {
      auto dpd = new DataPointDefinition()
      if (DataPointDefinition::getDataPointDefinitionFor(definition_,dpd,nullptr,defMap_))
        def_=dpd;
      }
      else delete dpd;
      }
    }
    else def_=*itr;
  }

  if (root.get(DATA, "").isArray()) {
    unsigned int size = root.get(DATA, "").size();
    for (unsigned int i = 0; i < size; i++)
    {
      //data_.push_back(DataPoint dp(root.get(DATA, "")[i]));
      if (def_) {
        type= def_->getType(i);
        op= def_->getOperation(i);
      }
      data_.push_back(DataPoint dp(root.get(DATA, "")[i],type,op));
    }
  }
}

/*
 *
 * Method implementation for the new multi-threaded model
 *
 * */

void DataPointCollection::trackMonitorable(std::vector<JsonMonitorable> &monitorables,
                                 std::string const& name,
                                 MonType type,
                                 OperationType op,
                                 EmptyMode em,
                                 SnapshotMode sm,
                                 bool isGlobal,
                                 size_t streams,
                                 bool forceSize);//false --> true to use full per-stream instances in LS history for types where only one can be used (sums..)

  assert(streams==1 && isGlobal);

  for (size_t streamid=0;streamid<streams;streamid++)
  {
    switch (op) {
      case OP_SUM:
        assert(type!=TYPE_STRING);
        if (type==TYPE_INT) monitorables.push_back(new IntJ(...));
        if (type==TYPE_DOUBLE) monitorables.push_back(new DoubleJ(...));
        break;
      case OP_AVG:
        assert(type!=TYPE_STRING);
        if (type==TYPE_INT) monitorables.push_back(new VectorJ<IntJ>(...));
        if (type==TYPE_DOUBLE) monitorables.push_back(new VectorJ<DoubleJ>(...));
        break;
      case OP_SAME:
        if (type==TYPE_STRING) monitorables.push_back(new StringJ(...));
        if (type==TYPE_INT) monitorables.push_back(new IntJ(...));
        if (type==TYPE_DOUBLE) monitorables.push_back(new DoubleJ(...));
        break;
      case OP_HISTO:
        assert(type!=TYPE_STRING);
        if (type==TYPE_INT) monitorables.push_back(new HistoJ<IntJ>(...));
        if (type==TYPE_DOUBLE) monitorables.push_back(new HistoJ<DoubleJ>(...));
        break;
      case OP_CAT:
        if (type==TYPE_STRING) monitorables.push_back(new VectorJ<StringJ>(...));
        if (type==TYPE_INT) monitorables.push_back(new VectorJ<IntJ>(...));
        if (type==TYPE_DOUBLE) monitorables.push_back(new VectorJ<DoubleJ>(...));
        break;
      case OP_BINARYAND:
      case OP_BINARYOR:
        assert(type!=TYPE_STRING && type!=TYPE_DOUBLE);
        if (type==TYPE_INT) monitorables.push_back(new IntJ(...));
        break;
      case OP_MERGE:
        break;//??TODO
      case OP_ADLER32:
        assert(op==OP_ADLER32 && isGlobal);
        assert(type!=TYPE_STRING && type!=TYPE_DOUBLE);
        if (type==TYPE_INT) monitorables.push_back(new VectorJ<IntJ>(...));
        break; 
      default:
        assert(0);
    }
  }
  data_.push_back(DataPoint dp(name,monitorables,type,op,em,sm,isGlobal,forceSize));
  dataMap_[name]=data_[data_.size()-1];
  //todo:build definition..
}

//decltype(lumiDataMap_)::iterator
std::map<unsigned int,std::vector<std::vector<JsonMonitorable*>>>::iterator DataPointCollection::getLumiDataItr(unsigned int ls) {

  //TODO:cache
  auto itr = getLumiDataItr();
  lumiDataMap_.find(ls);
  if (itr==lumiDataMap_.end()) {
    lumiDataMap_[ls]=new std::vector<std::vector<JsonMonitorable*>>;
    itr = lumiDataMap_.find(ls);
    for (size_t i=0;i<data_.size();i++)
      itr->push_back(new std::vector<JsonMonitorable>);
  }
  return itr;
}

void DataPointCollection::snap(unsigned int ls)
{
  auto itr = getLumiDataItr();
  for (size_t i=0;i<data_.size();i++)
    data_[i] d.snap(ls,itr->at(i));
}

void DataPointCollection::snapGlobalEOL(unsigned int ls)
{
  auto itr = getLumiDataItr();
  for (size_t i=0;i<data_.size();i++)
   data_[i].snapGlobalEoL(ls,itr->at(i));
}

void DataPointCollection::snapStreamEoL(unsigned int ls, unsigned int streamID)
{
  auto itr = getLumiDataItr();
  for (size_t i=0;i<data_.size();i++)
   data_[i].snapStreamEoL(ls,streamID,itr->at(i));
}

//call only when data for ls is snapped before
JsonMonitorable* DataPointCollection::mergeAndRetrieveValue(std::string const& name, unsigned int ls)
{
  auto itr = dataMap_.find(name);
  if (itr==dataMap_.end()) return nullptr;
  //assume the caller takes care of deleting the object
  return itr->mergeAndRetrieveValue(ls);
}

void DataPointCollection::mergeAndSerialize(Json::Value & root,unsigned int ls,bool initJsonValue)
{
  root[SOURCE] = source_;
  root[DEFINITION] = definition_;
  for (d : data_) {
      root[DATA].append(d.getJsonValue(ls));
  }
  //TODO:write string..
}

//wipe out data that will no longer be used
void DataPointCollection::discardCollected(unsigned int ls)
{
  auto itr = getLumiDataItr();
  if (itr==lumiDataMap_.end()) return;
  if (*itr) {
    for (v : **itr)
      if (v) {
        for (e : *v)
          if (e) delete e;
      delete v;
      }
    delete *itr;
  }
  lumiDataMap_.erase(itr);
}

