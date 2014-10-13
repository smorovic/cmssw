/*
 * FastMonitor.cc
 *
 *  Created on: Nov 27, 2012
 *      Author: aspataru
 */

#include "EventFilter/Utilities/interface/FastMonitor.h"
#include "EventFilter/Utilities/interface/JsonSerializable.h"
#include "EventFilter/Utilities/interface/FileIO.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>

using namespace jsoncollector;

FastMonitor::FastMonitor(std::string const& defPath, bool useSource) :
	defPath_(defPath),setSource_(setSource)
{
  //get host and PID info
  if (useSource)
    getHostAndPID(sourceInfo_);
  dpCollection_=new DataPointCollection(sourceInfo_,defPath_)
}

FastMonitor::~FastMonitor()
{
  delete dpCollection_;
}

//per-process variables
void FastMonitor::registerGlobalMonitorable(JsonMonitorable * monitorable,std::string const& name, MonType type, OperationType op, EmptyMode em, SnapshotMode sm, size_t streams)
{
  dpCollection_->trackMonitorable(monitorable,name,type,op,em,sm,streams);
}

//fast path: no merge operation is performed
void FastMonitor::registerFastMonitorable(JsonMonitorable *monitorable,std::string const& name)
{
  //alternative monitorable for fast mon
  dpCollection_->trackFastMonitorable(monitorable,name);
}

void FastMonitor::commit(std::vector<unsigned int> *streamLumisPtr)
{
    dpCollection_->setStreamLumiPtr(streamLumisPtr);
}

void FastMonitor::snap(unsigned int ls)
{
  dpCollection_->snapCurrent(ls);
}

//update for global variables as most of them are correct only at global EOL
void FastMonitor::snapGlobalEOL(unsigned int ls)
{
  recentSnaps_++;
  dpCollection_->snapGlobal(ls);
}

//update atomic per-stream vars(e.g. event counters) not updating time-based measurements (mini/microstate)
void FastMonitor::snapStreamEOL(unsigned int ls, unsigned int streamID)
{
  recentSnaps_++;
  dpCollection_->snapStreamEOL(ls, streamID);
}

std::string && FastMonitor::getCSVString()
{
  return dpCollection_->getFastCSV();
}

void FastMonitor::outputCSV(std::string const& path, std::string const& csvString)
{
  std::ofstream outputFile;
  outputFile.open(path.c_str(), std::fstream::out | std::fstream::trunc);
  outputFile << defPathFast_ << std::endl;
  outputFile << csvString << std::endl;
  outputFile.close();
}

//get one variable (caller must delete it later)
JsonMonitorable* FastMonitor::getMergedIntJForLumi(std::string const& name,unsigned int ls)
{
  return  dpCollection_->mergeAndRetrieveValue(name,ls);
}

bool FastMonitor::outputFullJSON(std::string const& path, unsigned int lumi, bool log)
{
  if (log)
    LogDebug("FastMonitor") << "SNAP updates -: " <<  recentSnaps_ << " (by timer: " << recentSnapsTimer_ 
                              << ") in lumisection ";

  recentSnaps_ = recentSnapsTimer_ = 0;

  Json::Value && serializeRoot;
  dpCollection_->mergeAndSerialize(serializeRoot,lumi);

  Json::StyledWriter writer;
  std::string && result = writer.write(serializeRoot);
  FileIO::writeStringToFile(path, result);
  return true;
}

void FastMonitor::discardCollected(unsigned int forLumi)
{
  dpCollection_->discardCollected(forLumi);
}

void FastMonitor::getHostAndPID(std::string& sHPid)
{
  std::stringstream hpid;
  int pid = (int) getpid();
  char hostname[128];
  gethostname(hostname, sizeof hostname);
  hpid << hostname << "_" << pid;
  sHPid = hpid.str();
}

