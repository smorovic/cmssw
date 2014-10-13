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


DataPoint::~DataPoint()
{
  if (buf_) delete[] buf_;
}

/*
 *
 * Method implementation for simple DataPoint usage
 *
 */


void DataPoint::serialize(Json::Value& root) const
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


void DataPoint::deserialize(Json::Value& root)
{
  source_ = root.get(SOURCE, "").asString();
  definition_ = root.get(DEFINITION, "").asString();
  MonType type = TYPE_UNKNOWN;
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
      data_.push_back(root.get(DATA, "")[i]);
      /*if (def_)
        type= def_->getType(i);
      switch(type) {
        case TYPE_UNKNOWN: 
        case TYPE_STRING: 
          data_.push_back(root.get(DATA, "")[i].asString());
        case TYPE_INT: 
          data_.push_back(root.get(DATA, "")[i].asInt());
        case TYPE_DOUBLE:
          data_.push_back(root.get(DATA, "")[i].asDouble());
      }*/
    }
  }
}

/*
 *
 * Method implementation for the new multi-threaded model
 *
 * */

void DataPoint::trackMonitorable(JsonMonitorable *monitorable,bool NAifZeroUpdates)
{
  name_=monitorable->getName();
  tracked_ = (void*)monitorable;
  if (dynamic_cast<IntJ*>(monitorable)) monType_=TYPE_INT;
  else if (dynamic_cast<DoubleJ*>(monitorable)) monType_=TYPE_DOUBLE;
  else if (dynamic_cast<StringJ*>(monitorable)) monType_=TYPE_STRING;
  else assert(0);
  NAifZeroUpdates_=NAifZeroUpdates;

}

void DataPoint::trackVectorInt(std::string const& name, std::vector<unsigned int>  *monvec, bool NAifZeroUpdates)
{
  name_=name;
  tracked_ = (void*)monvec;
  isStream_=true;
  monType_=TYPE_INT;
  NAifZeroUpdates_=NAifZeroUpdates;
  makeStreamLumiMap(monvec->size());
}

void DataPoint::trackVectorIntAtomic(std::string const& name, std::vector<AtomicMonInt*>  *monvec, bool NAifZeroUpdates)
{
  name_=name;
  tracked_ = (void*)monvec;
  isStream_=true;
  isAtomic_=true;
  monType_=TYPE_INT;
  NAifZeroUpdates_=NAifZeroUpdates;
  makeStreamLumiMap(monvec->size());
}

void DataPoint::makeStreamLumiMap(unsigned int size)
{
  for (unsigned int i=0;i<size;i++) {
    streamDataMaps_.push_back(MonPtrMap());
  }
}

void DataPoint::snap(unsigned int lumi)
{
  isCached_=false;
  if (isStream_) {
    if (monType_==TYPE_INT)
    {
      for (unsigned int i=0; i<streamDataMaps_.size();i++) {
        unsigned int streamLumi_=streamLumisPtr_->at(i);//get currently processed stream lumi
        unsigned int monVal;

#if ATOMIC_LEVEL>0
        if (isAtomic_) monVal = (static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(i)->load(std::memory_order_relaxed);
#else 
        if (isAtomic_) monVal = *((static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(i));
#endif
	else monVal = (static_cast<std::vector<unsigned int>*>(tracked_))->at(i);

	auto itr =  streamDataMaps_[i].find(streamLumi_);
	if (itr==streamDataMaps_[i].end())
	{
	  if (opType_==OPCAT) {
            if (*nBinsPtr_) {
              HistoJ<unsigned int> *nh = new HistoJ<unsigned int>(1,MAXUPDATES);
              nh->update(monVal);
              streamDataMaps_[i][streamLumi_] = nh;
            }
          }
          else if (opType_==OPHISTO) {
            if (*nBinsPtr_) {
              HistoJ<unsigned int> *nh = new HistoJ<unsigned int>(1,MAXUPDATES);
              nh->update(monVal);
              streamDataMaps_[i][streamLumi_] = nh;
            }
          }
          else {//default to SUM
            IntJ *nj = new IntJ;
            nj->update(monVal);
            streamDataMaps_[i][streamLumi_]= nj;
          }
        }
        else { 
          if (opType_==OPCAT) {
            if (*nBinsPtr_) {
              (static_cast<HistoJ<unsigned int> *>(itr->second.get()))->update(monVal);
            }
          }
          else if (opType_==OPHISTO) {
            if (*nBinsPtr_) {
              (static_cast<HistoJ<unsigned int> *>(itr->second.get()))->update(monVal);
            }
          }
          else {
            *(static_cast<IntJ*>(itr->second.get()))=monVal;
          }
        }
      }
    }
    else assert(monType_!=TYPE_INT);//not yet implemented, application error
  }
  else snapGlobal(lumi);
}

void DataPoint::snapGlobal(unsigned int lumi)
{
  isCached_=false;
  if (isStream_) return;
  auto itr = globalDataMap_.find(lumi);
  if (itr==globalDataMap_.end()) {
    if (monType_==TYPE_INT) {
      IntJ *ij = new IntJ;
      ij->update((static_cast<IntJ*>(tracked_))->value());
      globalDataMap_[lumi]=ij;
    }
    if (monType_==TYPE_DOUBLE) {
      DoubleJ *dj = new DoubleJ;
      dj->update((static_cast<DoubleJ*>(tracked_))->value());
      globalDataMap_[lumi]=dj;
    }
    if (monType_==TYPE_STRING) {
      StringJ *sj = new StringJ;
      sj->update((static_cast<StringJ*>(tracked_))->value());
      globalDataMap_[lumi]=sj;
    }
  } else { 
    if (monType_==TYPE_INT)
      static_cast<IntJ*>(itr->second.get())->update((static_cast<IntJ*>(tracked_))->value());
    else if (monType_==TYPE_DOUBLE)
      static_cast<DoubleJ*>(itr->second.get())->update((static_cast<DoubleJ*>(tracked_))->value());
    else if (monType_==TYPE_STRING)
      static_cast<StringJ*>(itr->second.get())->concatenate((static_cast<StringJ*>(tracked_))->value());
  }
}

void DataPoint::snapStreamAtomic(unsigned int lumi, unsigned int streamID)
{
  if (!isStream_ || !isAtomic_) return;
  isCached_=false;
  if (monType_==TYPE_UINT)
  {
    unsigned int monVal;
#if ATOMIC_LEVEL>0
    if (isAtomic_) monVal = (static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(streamID)->load(std::memory_order_relaxed);
#else 
    if (isAtomic_) monVal = *((static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(streamID));
#endif
    else monVal = (static_cast<std::vector<unsigned int>*>(tracked_))->at(streamID);

    auto itr =  streamDataMaps_[streamID].find(lumi);
    if (itr==streamDataMaps_[streamID].end()) //insert
    {
      if (opType_==OPHISTO) {
        if (*nBinsPtr_) {
          HistoJ<unsigned int> *h = new HistoJ<unsigned int>(1,MAXUPDATES);
          h->update(monVal);
          streamDataMaps_[streamID][lumi] = h;
        }
      }
      else {//default to SUM

        IntJ *h = new IntJ;
        h->update(monVal);
        streamDataMaps_[streamID][lumi] = h;
      }
    }
    else 
    { 
      if (opType_==OPHISTO) {
        if (*nBinsPtr_) {
          static_cast<HistoJ<unsigned int>*>(itr->second.get())->update(monVal);
        }
      }
      else
        *(static_cast<IntJ*>(itr->second.get()))=monVal;
    }
  }
  else assert(monType_!=TYPE_INT);//not yet implemented
}

std::string DataPoint::fastOutCSV()
{
  if (tracked_) {
    if (isStream_) {
      std::stringstream ss;
      if (isAtomic_) { 
#if ATOMIC_LEVEL>0
        ss << (unsigned int) (static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(fastIndex_)->load(std::memory_order_relaxed); 
#else
        ss << (unsigned int) *((static_cast<std::vector<AtomicMonInt*>*>(tracked_))->at(fastIndex_));
#endif 
        fastIndex_ = (fastIndex_+1) % (static_cast<std::vector<AtomicMonInt*>*>(tracked_))->size();
      }
      else {
        ss << (static_cast<std::vector<unsigned int>*>(tracked_))->at(fastIndex_);
        fastIndex_ = (fastIndex_+1) % (static_cast<std::vector<unsigned int>*>(tracked_))->size();
      }

      return ss.str();
    }
    return (static_cast<JsonMonitorable*>(tracked_))->toString();
  }
  return std::string("");
}

JsonMonitorable* DataPoint::mergeAndRetrieveValue(unsigned int lumi)
{
  assert(monType_==TYPE_INT && isStream_);//for now only support UINT and SUM for stream variables
  IntJ *newJ = new IntJ;
  for (unsigned int i=0;i<streamDataMaps_.size();i++) {
    auto itr = streamDataMaps_[i].find(lumi);
    if (itr!=streamDataMaps_[i].end()) {
      newJ->add(static_cast<IntJ*>(itr->second.get())->value());
    }
  }
  cacheI_=newJ->value();
  isCached_=true;
  return newJ;//assume the caller takes care of deleting the object
}

void DataPoint::mergeAndSerialize(Json::Value & root,unsigned int lumi,bool initJsonValue)
{
  if (initJsonValue) {
    root[SOURCE] = source_;
    root[DEFINITION] = definition_;
  }

  if (isDummy_) {
    root[DATA].append("N/A");
    return;
  }
  if (!isStream_) {
    auto itr = globalDataMap_.find(lumi);
    if (itr != globalDataMap_.end()) {
      root[DATA].append(itr->second.get()->toString());
    }
    else {
      if (NAifZeroUpdates_) root[DATA].append("N/A");
      else if (monType_==TYPE_STRING)  root[DATA].append("");
      else  root[DATA].append("0");
    }
    return;
  }
  else {
    assert(monType_==TYPE_UINT);
    if (isCached_) {
      std::stringstream ss;
      ss << cacheI_;
      root[DATA].append(ss.str());
      return;
    }
    if (opType_==OPCAT) {
      if (nBinsPtr_==nullptr) {
        root[DATA].append("N/A");
        return;
      }
      if (*nBinsPtr_>bufLen_) {
        if (buf_) delete[] buf_;
        bufLen_=*nBinsPtr_;
        buf_= new uint32_t[bufLen_];
      }
      memset(buf_,0,bufLen_*sizeof(uint32_t));
      unsigned int updates=0;
      for (unsigned int i=0;i<streamDataMaps_.size();i++) {
        auto itr = streamDataMaps_[i].find(lumi);
        if (itr!=streamDataMaps_[i].end()) {
          HistoJ <unsigned int>* monObj = static_cast<HistoJ<unsigned int>*>(itr->second.get());
          updates+=monObj->getUpdates();
          auto &hvec = monObj->value();
          for (unsigned int i=0;i<hvec.size();i++) {
            unsigned int thisbin=(unsigned int) hvec[i];
            if (thisbin<*nBinsPtr_) {
              buf_[thisbin]++;
            }
          }
        }
      }
      std::stringstream ss;
      Json::Value jsonVect;
      if (!*nBinsPtr_ || (!updates && NAifZeroUpdates_)) {
        std::stringstream ss;
        ss << "N/A";
        root[DATA].append(ss.str());
      }
      else {
        for (unsigned int i=0;i<*nBinsPtr_;i++) {
          std::stringstream ss;
          ss << buf_[i];
          jsonVect.append(ss.str());
        }
      }
        root[DATA].append(jsonVect);
      return;
    }
    else if (opType_==OPHISTO) {
    }
    else {//sum is default
      std::stringstream ss;
      unsigned int updates=0;
      unsigned int sum=0;
      for (unsigned int i=0;i<streamDataMaps_.size();i++) {
        auto itr = streamDataMaps_[i].find(lumi);
        if (itr!=streamDataMaps_[i].end()) {
          sum+=static_cast<IntJ*>(itr->second.get())->value();
          updates++;
        }
      }
      if (!updates && NAifZeroUpdates_) ss << "N/A";
      ss << sum;
      root[DATA].append(ss.str());
      return;
    }

  }
}

//wipe out data that will no longer be used
void DataPoint::discardCollected(unsigned int lumi)
{
  for (unsigned int i=0;i<streamDataMaps_.size();i++)
  {
    auto itr = streamDataMaps_[i].find(lumi);
    if (itr!=streamDataMaps_[i].end()) streamDataMaps_[i].erase(lumi);
  }

  auto itr = globalDataMap_.find(lumi);
  if (itr!=globalDataMap_.end())
    globalDataMap_.erase(lumi);
}

