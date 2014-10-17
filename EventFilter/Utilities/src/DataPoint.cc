/*
 * DataPoint.cc
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#include "EventFilter/Utilities/interface/DataPoint.h"

//#include <tbb/concurrent_vector.h>

#include <algorithm>
#include <assert.h>

//max collected updates per lumi
#define MAXUPDATES 200
#define MAXBINS

using namespace jsoncollector;

namespace jsoncollector {
  template class HistoJ<IntJ>;
  template class HistoJ<DoubleJ>;
  template class VectorJ<IntJ>;
  template class VectorJ<DoubleJ>;

}

const std::string DataPoint::SOURCE = "source";
const std::string DataPoint::DEFINITION = "definition";
const std::string DataPoint::DATA = "data";

//todo:optimize with move operator
DataPoint::DataPoint(TrackedMonitorable tracked, bool trackingInstance, bool fastMon): tracked_(tracked),fastMon_(fastMon)
{

  size_t len = tracked_.size();
  if (!len) {asert(tracked_.size());}
  MonitorableDefinition & md = tracked.md_;
  //set tracking option
  tracked_.isTracking_ = trackingInstance;
  if (fastMon_) return;

  //clone types from tracked object 
  monitorable_ = tracked_.monitorables_->at(0)->cloneAggregationType(md.getOperation(), md.getSnapshotMode(), tracked_.nbins_, tracked.expectedUpdates_, tracked.maxUpdates_);
  if (len>1) {
    assert(tracked_.isGlobal_);
    for (size_t i=0;i<tracked_.size();i++) {
      streamMonitorables_.push_back(tracked_.monitorables->at(i)->cloneAggregationType(md.getOperation(), md.getSnapshotMode(), tracked_.nbins_, tracked.expectedUpdates_, tracked.maxUpdates_));
    }
    useGlobalCopy_=false;
  }
}


//make non-tracking instance
DataPoint::DataPoint(TrackedMonitorable tracked, JsonMonitorable* aggregated): tracked_(tracked),fastMon_(false)
{
  tracked_.isTracking_=false;
  asert(aggregated_);
  monitorable_ = aggregated->cloneType();
  }
}


DataPoint::~DataPoint()
{
  if (monitorable_) delete monitorable_;
  for (m:streamMonitorables_) delete m;
  //TODO:fast
}

void DataPoint::reset()
{
  if (monitorable_) monitorable_->resetValue();
  for (m:streamMonitorables_) if (m) m->resetValue();
}

//get current tracked variable for fast-mon
std::string && DataPoint::snapAndGetFast()
{
  assert(tracked_.mon_ && tracked_.mon_->size());
  return tracked_.mon_->at(0)->toString();
}

void DataPoint::snapTimer()
{
  if (sm_!=SM_TIMER) return;
  isCached_=false;
  if (useGlobalCopy_) monitorable_->update(tracked_.at(0),sm);
  else for (size_t i=0;i<streammonitorables_.size();i++) streamMonitorables_[i]->update(tracked_.at(i),sm);
}

void DataPoint::snapGlobalEOL(unsigned int lumi)
{
  if (sm_!=SM_EOL || !isGlobal) return;
  isCached_=false;
   monitorable_->update(tracked_[0]);
}

void DataPoint::snapStreamEOL(unsigned int lumi)
{
  if (sm_!=SM_EOL || isGlobal) return;
  isCached_=false;
  if (useGlobalCopy_) monitorable_->update(tracked_.at(0),sm);
  else for (size_t i=0;i<streammonitorables_.size();i++) streamMonitorables_[i]->update(tracked_.at(i),sm);

}

//for FMS
Json::Value& DataPoint::getJsonValue()
{
  if (!isCached_) {
     isCached_=true;
    if (!useGlobalCopy) {
      monitorable_->resetUpdates();
      if (!JsonMonitorable::mergeData(monitorable_,streamMonitorables_,type_,op_)) {
        value_ = "N/A";
        return value_;
      }
    }
    if (!monitorable->getUpdates() && tracked_.md_.em_ = EM_NA)
        value_ = "N/A";
    else
      value_= monitorable->toJsonValue(value_);
  return value_;
}

//For FMS (processed)
JsonMonitorable* DataPoint::mergeAndRetrieveMonitorable()
{
  if (!useGlobalCopy && !isCached_)
    JsonMonitorable::mergeData(monitorable_,streamMonitorables_,type_,op_);
  return monitorable_->clone();
}

//for standalone binary
bool DataPoint::mergeAndSerializeMonitorables(Json::Value &root,std::vector<JsonMonitorable*>* vec)
{
  isCached_=true;
  monitorable_->resetUpdates();
  if (vec->size() && mergeMonitorables(vec)) {
    if ( tracked_.md_.getEmptyMode()==EM_NA && !monitorable_->getUpdates())
      value_ = "N/A";
    else
      value_= monitorable->toJsonValue(value_);
    root[DATA].append(value_);
    return true;
  }
  else {
    value_ = "N/A";
    root[DATA].append(value_);
    return false;
  }
}

//for stream modules
bool DataPoint::mergeMonitorables(std::vector<JsonMonitorable*> *vec)
{
  assert(useGlobalCopy_ && !tracked_.mon_);
  return JsonMonitorable::mergeData(monitorable_, vec, tracked_.md_.getType(), tracked_.md_.getOperation());
  cached_=true;
}

