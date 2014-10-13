/*
 * JsonMonitorable.h
 *
 *  Created on: Oct 29, 2012
 *      Author: aspataru
 */

#ifndef JSON_MONITORABLE_H
#define JSON_MONITORABLE_H

#include <string>
#include <sstream>
#include <vector>
#include <memory>
//#include "EventFilter/Utilities/interface/Utils.h"
#include <iostream>

namespace jsoncollector {

enum MonType  { TYPE_UNKNOWN, TYPE_INT, TYPE_DOUBLE, TYPE_STRING };
enum OperationType  { OP_UNKNOWN, OP_SUM, OP_AVG, OP_SAME, OP_HISTO, OP_CAT, OP_BINARYAND, OP_MERGE, OP_BINARYOR, OP_ADLER32 };

class JsonMonitorable {

public:

	JsonMonitorable() : updates_(0), notSame_(false) {}

	virtual ~JsonMonitorable() {}

	virtual std::string toString() const = 0;

	virtual void resetValue() = 0;

	unsigned int getUpdates() {return updates_;}
	
	bool getNotSame() {return notSame_;}

	virtual void setName(std::string name) {
		name_=name;
	}

	virtual std::string & getName() {
		return name_;
	}



protected:
	std::string name_;
	unsigned int updates_;
	bool notSame_;
};

class JsonMonPtr {
public:
	JsonMonPtr():ptr_(nullptr){}
	JsonMonPtr(JsonMonitorable*ptr):ptr_(ptr){}
	void operator=(JsonMonitorable* ptr ){ptr_=ptr;}
	~JsonMonPtr() {if (ptr_) delete ptr_;ptr_=nullptr;}
	JsonMonitorable* operator->() {return ptr_;}
	JsonMonitorable* get() {return ptr_;}
	//JsonMonPtr& operator=(JsonMonPtr& ) = delete;
	//JsonMonPtr& operator=(JsonMonPtr&& other){ptr_=other.ptr_;return *this;}
private:
	JsonMonitorable *ptr_;
};




class IntJ: public JsonMonitorable {

public:
	IntJ() : JsonMonitorable(), theVar_(0) {}
	IntJ(long val) : JsonMonitorable(), theVar_(val) {}

	virtual ~IntJ() {}

	virtual std::string toString() const {
		std::stringstream ss;
		ss << theVar_;
		return ss.str();
	}
	virtual void resetValue() {
		theVar_=0;
		updates_=0;
		notSame_=0;
	}
	void operator=(long sth) {
		theVar_ = sth;
		updates_=1;
		notSame_=0;
	}
	long & value() {
		return theVar_;
	}

	void update(long sth) {
		theVar_=sth;
		if (updates_ && theVar_!=sth) notSame_=true;
		updates_++;
	}

	void add(long sth) {
		theVar_+=sth;
		updates_++;
	}

private:
	long theVar_;
};


class DoubleJ: public JsonMonitorable {

public:
	DoubleJ() : JsonMonitorable(), theVar_(0) {}
	DoubleJ(double val) : JsonMonitorable(), theVar_(val) {}

	virtual ~DoubleJ() {}

	virtual std::string toString() const {
		std::stringstream ss;
		ss << theVar_;
		return ss.str();
	}
	virtual void resetValue() {
		theVar_=0;
		updates_=0;
		notSame_=0;
	}
	void operator=(double sth) {
		theVar_ = sth;
		updates_=1;
		notSame_=0;
	}
	double & value() {
		return theVar_;
	}
	void update(double sth) {
		theVar_=sth;
		if (updates_ && theVar_!=sth) notSame_=true;
		updates_++;
	}

private:
	double theVar_;
};


class StringJ: public JsonMonitorable {

public:
	StringJ() :  JsonMonitorable() {}

	virtual ~StringJ() {}

	virtual std::string toString() const {
		return theVar_;
	}
	virtual void resetValue() {
		theVar_=std::string();
		updates_ = 0;
		notSame_=0;
	}
	void operator=(std::string sth) {
		theVar_ = sth;
		updates_=1;
		notSame_=0;
	}
	std::string & value() {
		return theVar_;
	}
	void concatenate(std::string const& added) {
		if (!updates_)
		  theVar_=added;
		else
		theVar_+=","+added;
		updates_++;
	}
	void update(std::string const& newStr) {
		theVar_=newStr;
		updates_=1;
	}

private:
	std::string theVar_;
};

//vectors filled at time intervals
template<class T> class VectorJ: public JsonMonitorable {

public:
	VectorJ( int expectedUpdates = 1 , unsigned int maxUpdates = 0 ){
		expectedSize_=expectedUpdates;
		updates_ = 0;
		maxUpdates_ = maxUpdates;
		if (maxUpdates_ && maxUpdates_<expectedSize_) expectedSize_=maxUpdates_;
		vec_.reserve(expectedSize_);
	}
	virtual ~VectorJ() {}

	std::string toCSV() const {
		std::stringstream ss;
		for (unsigned int i=0;i<updates_;i++) {
			ss << vec_[i];
			if (i!=vec_.size()-1) ss<<",";
		}
		return ss.str();
	}

	virtual std::string toString() const {
		std::stringstream ss;
		ss << "[";
		if (vec_.size())
			for (unsigned int i = 0; i < vec_.size(); i++) {
				ss << vec_[i];
				if (i<vec_.size()-1) ss << ",";
			}
		ss << "]";
		return ss.str();
	}
	virtual void resetValue() {
		vec_.clear();
		vec_.reserve(expectedSize_);
		updates_=0;
	}
	void operator=(std::vector<T> & sth) {
		vec_ = sth;
	}

	std::vector<T> & value() {
		return vec_;
	}

	unsigned int getExpectedSize() {
		return expectedSize_;
	}

	unsigned int getMaxUpdates() {
		return maxUpdates_;
	}

	void setMaxUpdates(unsigned int maxUpdates) {
		maxUpdates_=maxUpdates;
		if (!maxUpdates_) return;
		if (expectedSize_>maxUpdates_) expectedSize_=maxUpdates_;
		//truncate what is over the limit
		if (maxUpdates_ && vec_.size()>maxUpdates_) {
			vec_.resize(maxUpdates_);
		}
		else vec_.reserve(expectedSize_);
	}

	unsigned int getSize() {
		return vec_.size();
	}

	void update(T val) {
		if (maxUpdates_ && updates_>=maxUpdates_) return;
		vec_.push_back(val);
		updates_++;
	}

private:
	std::vector<T> vec_;
	unsigned int expectedSize_;
	unsigned int maxUpdates_;
};


//fixed size histogram
template<class T> class HistoJ: public JsonMonitorable {

public:
	HistoJ( int length) {
                len_=length;
                histo_ = new std::vector<T>;
                for (unsigned int i=0;i<len;i++) histo_.push_back(0)
                allocated_=true;
	}
	HistoJ( std::vector<T>*histo) {
                len_ = histo->size();
                histo_ = histo;
	}
	virtual ~HistoJ() {
                if (allocated_)
                  delete histo_;
        }

	std::string toCSV() const {
		std::stringstream ss;
		for (unsigned int i=0;i<updates_;i++) {
			ss << histo_[i];
			if (i!=histo_.size()-1) ss<<",";
		}
		return ss.str();
	}

	virtual std::string toString() const {
		std::stringstream ss;
		ss << "[";
		if (histo_.size())
			for (unsigned int i = 0; i < histo_.size(); i++) {
				ss << histo_[i];
				if (i<histo_.size()-1) ss << ",";
			}
		ss << "]";
		return ss.str();
	}
	virtual void clear() {
                for (unsigned int i=0;i<len;i++) histo_[i]=0
	}

	std::vector<T> & value() {
		return *histo_;
	}

	unsigned int getSize() {
		return vec_.size();
	}

	void update(T val,size_t index) {
		if (index>=len) return;
		vec_[len]+=val;
	}

	void set(T val,size_t index) {
		if (index>=len) return;
		vec_[len]=val;
	}


private:
	std::vector<T> * histo_ = nullptr;
        allocated_ = false;
        unsigned int len_=0;
};




}

#endif
