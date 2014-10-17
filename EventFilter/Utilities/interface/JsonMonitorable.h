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
#include <zlib.h>

#define JSON_DEFAULT_MAXUPDATES 100

namespace jsoncollector {

enum MonType  { TYPE_UNKNOWN, TYPE_INT, TYPE_DOUBLE, TYPE_STRING };

enum OperationType  { OP_UNKNOWN, OP_SUM, OP_AVG, OP_SAME, OP_SAMPLE, OP_HISTO, OP_APPEND, OP_CAT, OP_BINARYAND, OP_BINARYOR, OP_ADLER32 };

enum EmptyMode = {EM_UNSET,EM_NA, EM_EMPTY};

enum SnapshotMode = {SM_UNSET,SM_TIMER, SM_EOL};

//TODO:resolve updates

class JsonMonitorable {

public:

	JsonMonitorable() : updates_(0), notSame_(false) {}

	virtual ~JsonMonitorable() {}

        virtual JsonMonitorable* clone() = 0;

        virtual JsonMonitorable* cloneType() = 0;

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0) = 0;

	virtual std::string toString() const = 0;

	virtual Json::Value && toJsonValue(char const& root) const  = 0;

	virtual void resetValue() = 0;

        virtual bool update(JsonMonitorable* mon,SnapshotMode sm) = 0;

	unsigned int getUpdates() {return updates_;}

        static void typeCheck(MonType type, OperationType op, SnapshotMode sm, JsonMonitorable const& mon);
        static bool typeAndOperationCheck(MonType type, OperationType op);
        static bool mergeData(JsonMonitorable *monitorable, std::vector<JsonMonitorable*>* vec,MonType type, OperationType op,bool final=true);
        static bool sumData<T>(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec);

	void resetUpdates() {updates_=0;notSame_=false;}

	bool getNotSame() {return notSame_;}

protected:
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

        virtual JsonMonitorable* clone() {
          IntJ* newVar = new IntJ;
          *newVar = *this;
          return newVar;
        }

        virtual JsonMonitorable* cloneType() {
          return new IntJ();
        }

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0);

	virtual std::string toString() const {
		std::stringstream ss;
		ss << theVar_;
		return ss.str();
	}

	virtual Json::Value && toJsonValue() const {
                Json::Value val = toString();
                return val;
	}

	virtual void resetValue() {
		theVar_=0;
		updates_=0;
		notSame_=0;
	}
        virtual bool update(JsonMonitorable* mon,SnapshotMode sm, OperationType op)
        {
                switch (op) {
                  case OP_SUM:
                  case OP_AVG:
                    add((static_cast<IntJ*>(mon)));
                    break;
                  case OP_SAME:
                  case OP_SINGLE:
                    update((static_cast<IntJ*>(mon)));
                    break;
                   //not yet in SM_TIMER mode
                  case OP_BINARYOR:
                    assert(0);
                    //binaryOr(static_cast<IntJ*>(mon));
                    break;
                  case OP_BINARYAND:
                    assert(0);
                    //binaryAnd(static_cast<IntJ*>(mon));
                    break;
                  case OP_ADLER32:
                    update((static_cast<IntJ*>(mon)));
                  default:
                    assert(0);
                }
        }

	void operator=(long sth) {
		theVar_ = sth;
		updates_=1;
		notSame_=0;
	}
	long & value() {
		return theVar_;
	}

	void update(long l) {
		if (updates_ && theVar_!=l) notSame_=true;
		theVar_=l;
		updates_++;
	}

	void update(IntJ* mon) {
		if (updates_ && theVar_!=mon->value()) notSame_=true;
		theVar_=mon->value();
		updates_++;
	}

	void add(long l) {
                if (!updates) theVar_=l;
		else thevar_+=l;
		updates_++;
	}

	void add(IntJ* mon) {
		if (updates_ && theVar_!=mon->value()) notSame_=true;
                if (!updates) theVar_=mon->value();
		else theVar_=+mon->value();
		updates_+=mon->getUpdates();
	}


	void binaryAnd(long l) {
                if (updates_) theVar_=theVar_ & l;
                else theVar_= l;
                updates_++;
        }

	void binaryAnd(IntJ* mon) {
                if (mon->getUpdates() && updates_) theVar_=theVar_ & mon->value();
                else if (mon->getUpdates()) theVar_= mon->value();
                else return;
                updates_+=mon->getUpdates();
        }

	void binaryOr(long l) {
                if (!updates) theVar_ = l;
                else theVar_ = theVar_ | l;
                updates_++;
        }

	void binaryOr(IntJ* mon) {
                if (!updates) theVar_ = mon->value();
                else theVar_=theVar_ | mon->value();
                updates_+=mon->getUpdates();
        }

        /*
	void adler32combine(IntJ* mon,size_t len2) {
                //TODO:we need size of 2 as well..
                return;
		//long adler2 = mon->value();
		//updates_+=mon->getUpdates();
	}*/

private:
	long theVar_;
};

class IntJAdler32: public IntJ {
public:

	IntJAdler32(long adler32, long len):
          IntJ(adler32),
          len_(len) {}

        long len() {return len_;}

	void adler32combine(IntJAdler32* mon) {
                if (!mon || !mon->len() || mon->value()<0) return;
                value()=adler32_combine(theVar,mon->len(),mon->value()&0xffffffff);
                len_+=mon->len();
	};
private:
        long len_;
}

class DoubleJ: public JsonMonitorable {

public:
	DoubleJ() : JsonMonitorable(), theVar_(0) {}
	DoubleJ(double val) : JsonMonitorable(), theVar_(val) {}

	virtual ~DoubleJ() {}

        virtual JsonMonitorable* clone() {
          DoubleJ* newVar = new DoubleJ;
          *newVar = *this;
          return newVar;
        }

        virtual JsonMonitorable* cloneType() {
          return new DoubleJ();
        }

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0);

	virtual std::string toString() const {
		std::stringstream ss;
		ss << theVar_;
		return ss.str();
	}

	virtual Json::Value && toJsonValue() const {
                Json::Value val = toString();
                return val;
	}

	virtual void resetValue() {
		theVar_=0;
		updates_=0;
		notSame_=0;
	}

        virtual bool update(JsonMonitorable* mon,SnapshotMode sm, OperationType op)
        {
                switch (op) {
                  case OP_SUM:
                  case OP_AVG:
                    add((static_cast<DoubleJ*>(mon)));
                    break;
                  case OP_SAME:
                  case OP_SINGLE:
                    update((static_cast<StringJ*>(mon)));
                    break;
                  default:
                    assert(0);
                }
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
		if (updates_ && theVar_!=sth) notSame_=true;
		theVar_=sth;
		updates_++;
	}

	void update(DoubleJ * s) {
		if (updates_ && theVar_!=s->value()) notSame_=true;
		theVar_=s->value();
		updates_++;
	}

	void add(double d ) {
		theVar_+=d;
		updates_++;
	}

	void add(DoubleJ * d) {
		theVar_+=d->value();
		updates_++;
	}

private:
	double theVar_;
};


class StringJ: public JsonMonitorable {

public:
	StringJ() :  JsonMonitorable() {}
	StringJ(std::string val) : JsonMonitorable(),theVar_(val), {}

	virtual ~StringJ() {}

        virtual JsonMonitorable* clone() {
          StringJ* newVar = new StringJ;
          *newVar = *this;
          return newVar;
        }

        virtual JsonMonitorable* cloneType() {
          return new StringJ();
        }

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0);

        virtual bool update(JsonMonitorable* mon,SnapshotMode sm, OperationType op)
        {
                switch (op) {
                  case OP_UNKNOWN:
                  case OP_SAME:
                  case OP_SINGLE:
                    update(mon);
                    break;
                  case OP_CAT:
                    concatenate(mon);
                    break;
                  default:
                    assert(0);
                }
        }

	virtual std::string toString() const {
		return theVar_;
	}

	virtual Json::Value && toJsonValue() const {
                Json::Value val = theVar_;
                return val;
	}

	virtual void resetValue() {
		theVar_=std::string();
		updates_ = 0;
		notSame_=false;
	}

	void operator=(std::string const& sth) {
		theVar_ = sth;
		updates_=1;
		notSame_=false;
	}

	std::string const& value() {
		return theVar_;
	}

	void concatenate(std::string const& added) {
		if (!updates_)
		  theVar_=added;
		else
                  if (added.size())
		    theVar_+=","+added;
		updates_++;
	}

	void concatenate(StringJ* s) {
		if (!updates_)
		  theVar_=s->value();
		else
                  if (s->value().size())
		    theVar_+=","+s->value();
		updates_+=s->getUpdates();
	}

	void update(std::string const& newStr) {
		if (updates_ && theVar_!=newStr) notSame_=true;
		theVar_=newStr;
		updates_=1;
	}

	void update(StringJ* s) {
		if (updates_ && theVar_!=s->value()) notSame_=true;
		theVar_=s->value();
		updates_=1;
	}


private:
	std::string theVar_;
};

//vectors filled at time intervals
template<class T> class VectorJ: public JsonMonitorable {

public:
	VectorJ( int expectedUpdates = 1 , unsigned int maxUpdates = 0 ): JsonMonitorable() {
		expectedUpdates_=expectedUpdates;
		maxUpdates_ = maxUpdates;
		if (maxUpdates_ && maxUpdates_<expectedUpdates_) expectedUpdates_=maxUpdates_;
		vec_.reserve(expectedUpdates_);
	}
	virtual ~VectorJ() {}

        virtual JsonMonitorable* clone() {
          VectorJ<T>* newVar = new VectorJ<T>(expectedUpdates_,maxUpdates_);
          *newVar = *this;
          return newVar;
        }

        virtual JsonMonitorable* cloneType() {
          return new VectorJ<T>(expectedUpdates_,maxUpdates_);
        }

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0) {
          return cloneType();
        }

	virtual std::string toString() const {
		std::stringstream ss;
		ss << "[";
		if (vec_.size())
			for (unsigned int i = 0; i < vec_.size(); i++) {
				ss << vec_[i].toString();
				if (i<vec_.size()-1) ss << ",";
			}
		ss << "]";
		return ss.str();
	}

	virtual Json::Value && toJsonValue() const {
                Json::Value val(Json::arrayValue);
		for (v: vec_) val.append(v.toString());
                return val;
	}

	virtual void resetValue() {
		vec_.clear();
		vec_.reserve(expectedUpdates_);
		updates_=0;
	}


        virtual void update(JsonMonitorable* mon,SnapshotMode sm, OperationType op)
        {
                if (sm==SM_TIMER) {
                  if (op==OP_CAT || op==OP_APPEND) updateWithScalar(static_cast<T>(mon));
                  else assert(0);
                }
                else updateWithVector(static_cast<VectorJ<T>>(mon));
        }

        void updateWithScalar(T* t) {
          //TODO
        }
        void updateWithVector(VectorJ<T>* t) {
          //TODO
        }

	void operator=(std::vector<T> & sth) {
		vec_ = sth;
	}

	std::vector<T> & value() {
		return vec_;
	}

	unsigned int getExpectedUpdates() {
		return expectedUpdates_;
	}

	unsigned int getMaxUpdates() {
		return maxUpdates_;
	}

	void setMaxUpdates(unsigned int maxUpdates) {
		maxUpdates_=maxUpdates;
		if (!maxUpdates_) return;
		if (expectedUpdates_>maxUpdates_) expectedUpdates_=maxUpdates_;
		//truncate what is over the limit
		if (maxUpdates_ && vec_.size()>maxUpdates_) {
			vec_.resize(maxUpdates_);
		}
		else vec_.reserve(expectedUpdates_);
	}

	unsigned int getSize() {
		return vec_.size();
	}

	void update(T val) {
		if (maxUpdates_ && updates_>=maxUpdates_) return;
		vec_.push_back(val);
		updates_++;
	}

protected:
	std::vector<T> vec_;
	size_t expectedUpdates_=0;
	size_t maxUpdates_ = JSON_DEFAULT_MAXUPDATES;
};


//fixed size histogram
template<class T> class HistoJ: public JsonMonitorable {

public:
	HistoJ( int length) : JsonMonitorable() : histo_(length), len_(length) {
                (void)static_cast<JsonMonitorable*>((T*)0);
	}
	HistoJ( std::vector<T>*histo) : JsonMonitorable() {
                (void)static_cast<JsonMonitorable*>((T*)0);
                len_ = histo.size();
                histo_ = histo;
	}
	virtual ~HistoJ() {
        }

        virtual JsonMonitorable* clone() {
          VectorJ<T>* newVar = new HistoJ<T>(len_);
          *newVar = *this;
          return newVar;
        }

        virtual JsonMonitorable* cloneType() {
          return new HistoJ<T>(len_);
        }

        virtual JsonMonitorable* cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins=0, size_t expectedUpdates=0, size_t maxUpdates=0) {
          return cloneType();
        }

	virtual std::string toString() const {
		std::stringstream ss;
		ss << "[";
		if (histo_.size())
			for (unsigned int i = 0; i < histo_.size(); i++) {
				ss << histo_[i].toString();
				if (i<histo_.size()-1) ss << ",";
			}
		ss << "]";
		return ss.str();
	}

	virtual Json::Value && toJsonValue() const {
                Json::Value val(Json::arrayValue);
		for (h: histo_) val.append(histo_[i].toString());
                return val;
	}

	virtual void resetValue() {
                for (unsigned int i=0;i<len;i++) histo_[i]=0
	}

        virtual bool update(JsonMonitorable* mon,SnapshotMode sm, OperationType op)
        {
                if (sm==SM_TIMER) {
                  if (op==OP_HISTO) return updateWithScalar(static_cast<T>(mon));
                  else assert(0);
                }
                else return updateWithHisto(static_cast<HistoJ<T>>(mon));
        }

        //todo:check range
        bool updateWithScalar(T* t) {
          if (!updates_) {
            if (t->getUpdates())
              histo_[t->value()]=T->value();
          }
          else if  (t->getUpdates())
            histo_[t->value()]+=T->value();
          updates_+=t->getUpdates();
          return true;
        }

        //todo:check range
        bool updateWithHisto(HistoJ<T>* t) {
          if (!updates_) {
            if (t->getUpdates())
              for (size_t i=0;i<t->value().size();i++)
                histo_[i]=t->value()[i];
          }
          else if (t->getUpdates()) {
            for (size_t i=0;i<t->value().size();i++)
              histo_[i]+=t->value()[i];
          }
          updates_+=t->getUpdates();
          return true;
        }

	std::vector<T> & value() {
		return histo_;
	}

	unsigned int getSize() {
		return vec_.size();
	}

	void update(T val, size_t index) {
		if (index>=len) return;
		vec_[len]+=val;
	}

	void set(T val, size_t index) {
		if (index>=len) return;
		vec_[len]=val;
	}

private:
	std::vector<T> histo_ = nullptr;
        size_t len_=0;
};

template class HistoJ<IntJ>;
template class HistoJ<DoubleJ>;
template class VectorJ<IntJ>;
template class VectorJ<DoubleJ>;
template class VectorJ<StringJ>;


}

#endif
