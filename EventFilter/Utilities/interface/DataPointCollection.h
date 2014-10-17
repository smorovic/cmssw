/*
 * DataPoint.h
 *
 *  Created on: Sep 24, 2012
 *      Author: aspataru
 */

#ifndef DATAPOINT_H_
#define DATAPOINT_H_

#include "EventFilter/Utilities/interface/DataPoint.h"
#include "EventFilter/Utilities/interface/JsonSerializable.h"

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <stdint.h>
#include <assert.h>

namespace jsoncollector {

//typedef std::map<unsigned int,JsonMonPtr> MonPtrMap;
typedef std::map<unsigned int,std::vector<JsonMonPtr>> MonPtrMapVector;


template class HistoJ<unsigned int>;
template class HistoJ<double>;
template class VectorJ<unsigned int>;
template class VectorJ<double>;

class DataPoint: public JsonSerializable {

public:

	DataPointCollection() { }

	DataPointCollection(std::vector<TrackedMonitorable> *,bool fastMon=false);

	DataPointCollection(DataPointDefinition const* def):def_(def) {}

        DataPointCollection::DataPointCollection(std::string const& definition, std::map<std::string,DataPointDefinition> *defMap=nullptr);

	~DataPoint();

	/**
	 * JSON serialization procedure for this class
	 */

	virtual void serialize(Json::Value& root) const;

	/**
	 * JSON deserialization procedure for this class
	 */
	virtual void deserialize(Json::Value& root);

	std::string& getDefinition() const {return definition_;}
        const DataPointDefinition* getDPD() const {return def_;}
	std::vector<DataPoint> const& getData() const {return data_;}

	//set to track a variable
	JsonMonitorable trackMonitorable(std::string const& name, MonType type, OperationType op, EmptyMode em, SnapshotMode sm, bool isVector, size_t streams);

	//take new update for lumi
	void snapTimer(unsigned int ls);
	void snapGlobalEOL(unsigned int ls);
	void snapStreamEOL(unsigned int ls, unsigned int streamID);

	//only used if per-stream DP (should use non-atomic vector here)
	void setStreamLumiPtr(std::vector<unsigned int> *streamLumiPtr) {
	  streamLumisPtr_=streamLumiPtr;
	}

	//fastpath (not implemented now)
	std::string fastOutCSV();

	//get everything collected prepared for output
	void mergeAndSerialize(Json::Value& jsonRoot, unsigned int lumi, bool initJsonValue);

	//cleanup lumi
	void discardCollected(unsigned int forLumi);

	//std::string const& getName() {return name_;}

        void updateDefinition(std::string const& definition) {definition_=definition;}

        void setDefinitionMap(std::map<std::string,DataPointDefinition*> *defMap) {defMap_=defMap;}

        void DataPoint const& getDataAt(size_t index) const {
          if (index>=data_.size()) return nullptr;
          return data_[index];
        }

	// JSON field names
	static const std::string SOURCE;
	static const std::string DEFINITION;
	static const std::string DATA;

protected:
	//for simple usage
	std::string source_;
	std::string definition_;
        std::map<std::string,DataPointDefinition*> *defMap_=nullptr;
        DataPointDefinition *def_=nullptr;

	std::vector<DataPoint> data_;
        std::vector<MonitorableDefinition> *trackedVector_ = nullptr;
        bool fastMon_=false;
	//std::string name_;//?should be runX, lumi-->, type--> suffix-->


};
}

#endif /* DATAPOINT_H_ */
