/** \file
 *
 *  \author N. Amapane - CERN
 */

#include "DTHFakeReader.h"
#include "DataFormats/FEDRawData/interface/FEDHeader.h"
#include "DataFormats/FEDRawData/interface/FEDTrailer.h"
#include "DataFormats/FEDRawData/interface/FEDNumbering.h"
#include "DataFormats/TCDS/interface/TCDSRaw.h"

#include "EventFilter/Utilities/interface/GlobalEventNumber.h"

#include "DataFormats/FEDRawData/interface/FEDRawData.h"

#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "CLHEP/Random/RandGauss.h"

#include <cmath>
#include <sys/time.h>
#include <cstring>
#include <cstdlib>
#include <chrono>

//using namespace edm;
namespace evf {

  constexpr unsigned minOrbitBx = 1;
  constexpr unsigned maxOrbitBx = 2464;
  constexpr unsigned avgEventsPerOrbit = 70;
  constexpr unsigned h_size = 8;//TODO: get from header/trailer class
  constexpr unsigned t_size = 8;
  constexpr double rndFactor = (maxOrbitBx - minOrbitBx + 1) / (double(avgEventsPerOrbit) *RAND_MAX);


  DTHFakeReader::DTHFakeReader(const edm::ParameterSet& pset)
    : fillRandom_(pset.getUntrackedParameter<bool>("fillRandom", false)),
      meansize_(pset.getUntrackedParameter<unsigned int>("meanSize", 1024)),
      width_(pset.getUntrackedParameter<unsigned int>("width", 1024)),
      injected_errors_per_million_events_(pset.getUntrackedParameter<unsigned int>("injectErrPpm", 0)),
      sourceIdList_(pset.getUntrackedParameter<std::vector<unsigned int>>("sourceIdList", std::vector<unsigned int>())),
      modulo_error_events_(injected_errors_per_million_events_ ? 1000000 / injected_errors_per_million_events_
                                                             : 0xffffffff) {
    if (fillRandom_) {
      //intialize random seed
      auto time_count =
          static_cast<long unsigned int>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
      std::srand(time_count & 0xffffffff);
    }
    produces<FEDRawDataCollection>();
  }


  void DTHFakeReader::fillRawData(edm::Event& e, FEDRawDataCollection*& data) {

    // a null pointer is passed, need to allocate the fed collection (reusing it as container)
    data = new FEDRawDataCollection();
    //auto ls = e.luminosityBlock();
    //this will be used as orbit counter
    edm::EventNumber_t orbitId = e.id().event();

    //generate eventID. Orbits start from 0 or 1?
    std::vector<uint64_t> eventIdList_;
    std::map<unsigned int, std::map<uint64_t, uint32_t>> randFedSizes;
    for (auto sourceId : sourceIdList_) {
      randFedSizes[sourceId] = std::map<uint64_t, uint32_t>();
    }

    //randomize which orbit was accepted
    for (unsigned i=minOrbitBx; i<= maxOrbitBx; i++) {
      if ((std::rand() * rndFactor) < 1) {
        uint64_t eventId = orbitId * maxOrbitBx + i;
        eventIdList_.push_back(eventId);
        for (auto sourceId : sourceIdList_) {
          float logsiz = CLHEP::RandGauss::shoot(std::log(meansize_), std::log(meansize_) - std::log(width_ / 2.));
          size_t size = int(std::exp(logsiz));
          size -= size % 16;  // all blocks aligned to 128 bit words (with header+trailer being 16, this remains valid)
          randFedSizes[sourceId][eventId] = size;
        }
      }
    }

    for (auto sourceId : sourceIdList_) {
      FEDRawData& feddata = data->FEDData(sourceId);

      auto size = sizeof(DTHOrbitHeader_v1);
      for (auto eventId : eventIdList_)
        size += randFedSizes[sourceId][eventId] + h_size + t_size + sizeof(DTHFragmentTrailer);
      feddata.resize(size);

      uint64_t fragmentsSize =  sizeof(DTHOrbitHeader_v1);
      uint32_t runningChecksum = 0xffffffffU;
      for (auto eventId : eventIdList_) {
        unsigned char* fedaddr = feddata.data() + fragmentsSize;
        fragmentsSize += fillFED(fedaddr, sourceId, eventId, randFedSizes[sourceId][eventId], runningChecksum);
      }
      fragmentsSize = fragmentsSize >> 4;
      //in place construction
      new (feddata.data()) DTHOrbitHeader_v1(sourceId, orbitId, e.id().run(), fragmentsSize, eventIdList_.size(), runningChecksum, 0);
    }
  }


  void DTHFakeReader::produce(edm::Event& e, edm::EventSetup const& es) {
    edm::Handle<FEDRawDataCollection> rawdata;
    FEDRawDataCollection* fedcoll = nullptr;
    fillRawData(e, fedcoll);
    std::unique_ptr<FEDRawDataCollection> bare_product(fedcoll);
    e.put(std::move(bare_product));
  }


  uint32_t DTHFakeReader::fillFED(unsigned char* buf, const int sourceId, edm::EventNumber_t eventId, uint32_t size, uint32_t &crc32c) {
    // Generate size...
    const unsigned h_size = 8;
    const unsigned t_size = 8;

    //header+trailer+payload
    uint32_t totsize = size + h_size + t_size + sizeof(DTHFragmentTrailer);

    // Generate header
    //FEDHeader::set(feddata.data(),
    FEDHeader::set(buf,
                   1,            // Trigger type
                   eventId,  // LV1_id (24 bits)
                   0,            // BX_id
                   sourceId);       // source_id

    // Payload = all 0s or random
    if (fillRandom_) {
      //fill FED with random values
      size_t size_ui = size - size % sizeof(unsigned int);
      for (size_t i = 0; i < size_ui; i += sizeof(unsigned int)) {
        *((unsigned int*)(buf + h_size + i)) = (unsigned int)std::rand();
      }
      //remainder
      for (size_t i = size_ui; i < size; i++) {
        *(buf + h_size + i) = std::rand() & 0xff;
      }
    }

    // Generate trailer
    int crc = 0;  // FIXME : get CRC16
    FEDTrailer::set(buf + h_size + size,
                    size / 8 + 2,  // in 64 bit words
                    crc,
                    0,   // Evt_stat
                    0);  // TTS bits

    //TODO: accumulate crc32 checksum
    //crc32c = 0;

    void * dthTrailerAddr = buf + h_size + t_size + size;
    //TODO:write DRH trailer
    new (dthTrailerAddr) DTHFragmentTrailer(h_size + t_size + size, 0, crc, eventId);
    return totsize;
  }

  void DTHFakeReader::beginLuminosityBlock(edm::LuminosityBlock const& iL, edm::EventSetup const& iE) {
    std::cout << "DTHFakeReader begin Lumi " << iL.luminosityBlock() << std::endl;
    fakeLs_ = iL.luminosityBlock();
  }

  void DTHFakeReader::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
    edm::ParameterSetDescription desc;
    desc.setComment("Injector of generated DTH raw orbit fragments for DSAQ testing");
    desc.addUntracked<bool>("fillRandom", false);
    desc.addUntracked<unsigned int>("meanSize", 1024);
    desc.addUntracked<unsigned int>("width", 1024);
    desc.addUntracked<unsigned int>("injectErrPpm", 1024);
    desc.addUntracked<std::vector<unsigned int>>("sourceIdList", std::vector<unsigned int>());
    descriptions.add("DTHFakeReader", desc);
  }
} //namespace evf
