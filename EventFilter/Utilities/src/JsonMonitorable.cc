#include "EventFilter/Utilities/interface/JsonMonitorable.h"

//asserts if combination of operation, monitorable type and JsonMonitorable is now allowed
void JsonMonitorable::typeCheck(MonType type, OperationType op, SnapshotMode sm, JsonMonitorable const& mon)
{
  assert(OP_UNKNOWN);
  if (dynamic_cast<IntJ>(mon)) {
    assert(type==TYPE_INT);
    assert(op==OP_SUM || op==OP_AVG || op== OP_SAME || op== OP_SAMPLE || op == OP_BINARYAND || op == OP_BINARYOR || op == OP_ADLER32 || ((op==OP_APPEND || OP==OP_HISTO)&& sm==SM_TIMER));
  }
  else if (dynamic_cast<DoubleJ>(mon)) {
    assert(type==TYPE_DOUBLE);
    assert(op==OP_SUM || op==OP_AVG || op== OP_SAME || op== OP_SAMPLE || (op==OP_APPEND && sm==SM_TIMER));
  }
  else if (dynamic_cast<StringJ>(mon)) {
    assert(type==TYPE_STRING);
    assert(op==OP_SAME || op==OP_SAMPLE || op==OP_UNKNOWN || op==OP_CAT || (op==OP_APPEND || && sm==SM_TIMER));
  }
  else if (dynamic_cast<VectorJ<IntJ>>(mon)) {
    assert(type==TYPE_INT);
    assert(op==OP_APPEND);
    assert(sm==SM_EOL);
  }
  else if (dynamic_cast<VectorJ<DoubleJ>>(mon)) {
    assert(type==TYPE_DOUBLE);
    assert(op==OP_APPEND);
    assert(sm==SM_EOL);
  }
  else if (dynamic_cast<VectorJ<StringJ>>(mon)) {
    assert(type==TYPE_STRING);
    assert(op==OP_APPEND);
    assert(sm==SM_EOL);
  }
  else if (dynamic_cast<HistoJ<IntJ>>(mon)) {
    assert(type==TYPE_INT);
    assert(op==OP_HISTO);
    assert(sm==SM_EOL);
  }
  else assert(0);
/*  else if (dynamic_cast<HistoJ<DoubleJ>>(mon)) {
    assert(0);//currently unsupported
    //assert(type==TYPE_DOUBLE);
    //assert(op==OP_HISTO);
    //assert(sm!=SM_TIMER);
  }
  else if (dynamic_cast<HistoJ<StringJ>>(mon)) {assert(0);}
*/
}

//for validation of deserialized content
bool JsonMonitorable::typeAndOperationCheck(MonType type, OperationType op)
{
  switch (op) {
    case OP_CAT:
      if (type==TYPE_STRING) return true;
      return false;
    case OP_SUM:
    case OP_AVG:
    case OP_HISTO:
      if (type==TYPE_INT || type==TYPE_DOUBLE) return true;
      return false;
    case OP_UNKNOWN:
    case OP_APPEND:
    case OP_SAME:
    case OP_SAMPLE:
      return true;
    case OP_BINARYAND:
    case OP_BINARYOR:
    case OP_ADLER32:
      if (type==TYPE_INT) return true;
      return false;
    default:
      return false;
  }
}

bool JsonMonitorable::mergeData(JsonMonitorable *monitorable, std::vector<JsonMonitorable*>* vec,MonType type, OperationType op,bool final=true)
{

  if (!vec->size()) {
    monitorable->resetValue();
    return false;
  }

  switch (type) {
    case TYPE_INT:
        switch (op) {
            case OP_UNKNOWN;
            case OP_SUM;
                for (v : *vec) static_cast<IntJ*>(monitorable)->add(static_cast<IntJ*>(v).value());
                break;
            case OP_AVG;
                for (v : *vec) static_cast<IntJ*>(monitorable)->add(static_cast<IntJ*>(v).value());
                if (final) static_cast<IntJ*>(monitorable)->normalize();
                break;
            case OP_SAME;
                for (v:*vec) static_cast<IntJ>*(monitorable)->update(v);
                break;
            case OP_SAMPLE:
                if vec->size() static_cast<IntJ*>(monitorable)->update(vec->at(vec->size()-1));//take last
                break;
            case OP_HISTO;
                for (v : *vec) static_cast<HistoJ<IntJ>*>(monitorable)->add(static_cast<HistoJ<IntJ>*>(v).value());
                break;
            case OP_APPEND:
                for (v:*vec) static_cast<VectorJ<IntJ>*>(monitorable)->append(v)
                break;
            case OP_BINARYAND:
                for (v:*vec) static_cast<IntJ*>(monitorable)->binaryAnd(v)
                break;
            case OP_BINARYOR:
                for (v:*vec) static_cast<IntJ*>(monitorable)->binaryOr(v)
                break;
            case OP_ADLER32:
                for (v:*vec) static_cast<IntJ>*(monitorable)->update(v);//combination not handled yet
                break;
            default:
                return false;
        }
        break;

    case TYPE_DOUBLE:
        switch (op) {
            case OP_UNKNOWN;
            case OP_SUM;
                for (v : *vec) static_cast<DoubleJ*>(monitorable)->add(static_cast<DoubleJ*>(v).value());
                break;
            case OP_AVG;
                for (v : *vec) static_cast<DoubleJ*>(monitorable)->add(static_cast<DoubleJ*>(v).value());
                if (final) static_cast<DoubleJ*>(monitorable)->normalize();
                break;
            case OP_SAME;
                for (v:*vec) static_cast<DoubleJ>*(monitorable)->update(v);
                break;
            case OP_SAMPLE:
                if vec->size() static_cast<DoubleJ*>(monitorable)->update(vec->at(vec->size()-1));//take last
                break;
            case OP_APPEND:
                for (v:*vec) static_cast<VectorJ<DoubleJ>*>(monitorable)->append(v)
                break;
            default:
                return false;
        }
        break;

    case TYPE_STRING:
        switch (op) {
            case OP_SAME;
                for (v:*vec) static_cast<StringJ>*(monitorable)->update(v);
                break;
            case OP_SAMPLE:
                if vec->size() static_cast<StringJ*>(monitorable)->update(vec->at(vec->size()-1));//take last
                break;
            case OP_APPEND:
                for (v:*vec) static_cast<VectorJ<StringJJ>*>(monitorable)->append(v)
                break;
            case OP_UNKNOWN;
            case OP_CAT:
                for (v:*vec) static_cast<StringJ*>(monitorable)->concatenate(v);
                break;
            default:
                return false;
        }
        break;
    default:
        return false;
    }
    return true;
  }

bool JsonMonitorable::sumData<T>(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)

  for (v : *vec) 
    static_cast<T*>(monitorable)->add(static_cast<T*>(v).value());
  return true;
}

//cat across multiple monitorables
static bool JsonMonitorable::appendData<T>(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  for (v:*vec) static_cast<T*>(monitorable)->append(v)
  return true;
}

static bool JsonMonitorable::catData(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  for (v:*vec)
    static_cast<StringJ*>(monitorable)->concatenate(v);
  return true;
}

static bool JsonMonitorable::sameData<T>(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  for (v:*vec)
    static_cast<T>*(monitorable)->update(v);
  return true;
}

static bool JsonMonitorable::sampleData<T>(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  if vec->size()
    static_cast<T*>(monitorable)->update(vec->at(vec->size()-1));//take last
  return true;
}

static bool JsonMonitorable::binaryAndData(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  for (v:*vec)
    static_cast<IntJ*>(monitorable)->binaryAnd(v);
  return true;
}

static bool JsonMonitorable::binaryOrData(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  //set initial..
  for (v:*vec)
    static_cast<IntJ*>(monitorable)->binaryOr(v);
  return true;
}

//non-commutative operation
static bool JsonMonitorable::adler32Data(JsonMonitorable *monitorable, std::vector<JsonMonitorable*> *vec)
{
  for (v:*vec)
    //static_cast<IntJ*>(monitorable)->adler32combine(v);//TODO
    static_cast<IntJ*>(monitorable)->update(v);
  return true;
}

//initialize aggregation vector types for certain operations in timer mode
virtual JsonMonitorable* IntJ::cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins, size_t expectedUpdates, size_t maxUpdates) {
  switch (sm) {
    case SM_TIMER:
      if (op==OP_CAT || op==OP_APPEND)
	return new VectorJ<IntJ>(expectedUpdats,maxUpdates);
      if (op==OP_HISTO)
	return new HistoJ<IntJ>(bins);
    default:
      return new IntJ(theVar_);
  }
}

virtual JsonMonitorable* DoubleJ::cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins, size_t expectedUpdates, size_t maxUpdates) {
  switch (sm) {
    case SM_TIMER:
      if (op==OP_CAT || op==OP_APPEND)
	return new VectorJ<DoubleJ>(expectedUpdats,maxUpdates);
      if (op==OP_HISTO)
	return new HistoJ<DoubleJ>(bins);
    default:
      return new DoubleJ(theVar_);
  }
}

virtual JsonMonitorable* StringJ::cloneAggregationType(OperationType op, SnapshotMode sm, size_t bins, size_t expectedUpdates, size_t maxUpdates) {
  switch (sm) {
    case SM_TIMER:
      if (op==OP_CAT || op==OP_APPEND)
	return new VectorJ<IntJ>(expectedUpdats,maxUpdates);
    default:
      return new StringJ(theVar_);
  }
}

