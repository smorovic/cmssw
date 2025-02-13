#include "EventFilter/Utilities/interface/DAQSource.h"
#include "EventFilter/Utilities/interface/DAQSourceModelsDTH.h"

#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <bitset>

#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "DataFormats/FEDRawData/interface/FEDHeader.h"
#include "DataFormats/FEDRawData/interface/FEDTrailer.h"
#include "DataFormats/FEDRawData/interface/FEDRawDataCollection.h"

#include "DataFormats/TCDS/interface/TCDSRaw.h"

#include "FWCore/Framework/interface/Event.h"
#include "EventFilter/Utilities/interface/GlobalEventNumber.h"
#include "EventFilter/Utilities/interface/DAQSourceModels.h"
#include "EventFilter/Utilities/interface/DAQSource.h"

#include "EventFilter/Utilities/interface/AuxiliaryMakers.h"

#include "DataFormats/Provenance/interface/EventAuxiliary.h"
#include "DataFormats/Provenance/interface/EventID.h"
#include "DataFormats/Provenance/interface/Timestamp.h"
#include "EventFilter/Utilities/interface/crc32c.h"

using namespace evf;

void DataModeDTH::readEvent(edm::EventPrincipal& eventPrincipal) {
  std::unique_ptr<FEDRawDataCollection> rawData(new FEDRawDataCollection);
  edm::Timestamp tstamp = fillFEDRawDataCollection(*rawData);

  edm::EventID eventID = edm::EventID(daqSource_->eventRunNumber(), daqSource_->currentLumiSection(), nextEventID_);
  edm::EventAuxiliary aux(
      eventID, daqSource_->processGUID(), tstamp, isRealData(), edm::EventAuxiliary::PhysicsTrigger);
  aux.setProcessHistoryID(daqSource_->processHistoryID());
  daqSource_->makeEventWrapper(eventPrincipal, aux);

  std::unique_ptr<edm::WrapperBase> edp(new edm::Wrapper<FEDRawDataCollection>(std::move(rawData)));
  eventPrincipal.put(
      daqProvenanceHelpers_[0]->branchDescription(), std::move(edp), daqProvenanceHelpers_[0]->dummyProvenance());
  eventCached_ = false;
}

edm::Timestamp DataModeDTH::fillFEDRawDataCollection(FEDRawDataCollection& rawData) {
  //generate timestamp for this event until parsing of TCDS2 data is available
  edm::TimeValue_t time;
  timeval stv;
  gettimeofday(&stv, nullptr);
  time = stv.tv_sec;
  time = (time << 32) + stv.tv_usec;
  edm::Timestamp tstamp(time);

  for (size_t i = 0; i < eventFragments_.size(); i++) {
    auto fragTrailer = eventFragments_[i];
    uint8_t* payload = (uint8_t*)fragTrailer->payload();
    auto fragSize = fragTrailer->payloadSizeBytes();
    /*
    //Slink header and trailer
    assert(fragSize >= (FEDTrailer::length + FEDHeader::length));
    const FEDHeader fedHeader(payload);
    const FEDTrailer fedTrailer((uint8_t*)fragTrailer - FEDTrailer::length);
    const uint32_t fedSize = fedTrailer.fragmentLength() << 3;  //trailer length counts in 8 bytes
    const uint16_t fedId = fedHeader.sourceID();
*/

    //SLinkRocket header and trailer
    if (fragSize < sizeof(SLinkRocketTrailer_v3) + sizeof(SLinkRocketHeader_v3))
      throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Invalid fragment size: " << fragSize;

    const SLinkRocketHeader_v3* fedHeader = (const SLinkRocketHeader_v3*)payload;
    const SLinkRocketTrailer_v3* fedTrailer =
        (const SLinkRocketTrailer_v3*)((uint8_t*)fragTrailer - sizeof(SLinkRocketTrailer_v3));

    //check SLR trailer first as it comes just before fragmen trailer
    if (!fedTrailer->verifyMarker())
      throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Invalid SLinkRocket trailer";
    if (!fedHeader->verifyMarker())
      throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Invalid SLinkRocket header";

    const uint32_t fedSize = fedTrailer->eventLenBytes();
    const uint16_t fedId = fedHeader->sourceID();

    /*
     *  @SM: CRC16 in trailer was not checked up to Run3, no need to do production check
     *  if we already check orbit CRC32. If CRC16 check is to be added,
     *  in phase1 crc16 was calculated on sequential 64-byte little-endian words
     *  (see FWCore/Utilities/interface/CRC16.h).
     *  See also optimized pclmulqdq implementation in XDAQ.
     *  Note: check if for phase-2 crc16 is still based on 8-byte words
    */
    //const uint32_t crc16 = fedTrailer->crc();

    if (fedSize != fragSize)
      throw cms::Exception("DAQSource::DAQSourceModelsDTH")
          << "Fragment size mismatch. From DTHTrailer: " << fragSize << " and from SLinkRocket trailer: " << fedSize;
    FEDRawData& fedData = rawData.FEDData(fedId);
    fedData.resize(fedSize);
    memcpy(fedData.data(), payload, fedSize);  //copy with header and trailer
  }
  return tstamp;
}

std::vector<std::shared_ptr<const edm::DaqProvenanceHelper>>& DataModeDTH::makeDaqProvenanceHelpers() {
  //use also FRD data collection
  daqProvenanceHelpers_.clear();
  daqProvenanceHelpers_.emplace_back(std::make_shared<const edm::DaqProvenanceHelper>(
      edm::TypeID(typeid(FEDRawDataCollection)), "FEDRawDataCollection", "FEDRawDataCollection", "DAQSource"));
  return daqProvenanceHelpers_;
}

void DataModeDTH::makeDataBlockView(unsigned char* addr, RawInputFile* rawFile) {

  //addr points to beginning of the main file orbit block

  //get file array info
  auto numFiles = rawFile->fileSizes_.size();

  //initialize address tracking for files in the buffer: add primary file

  auto buf = rawFile->chunks_[0]->buf_;

  //all fragment addresses could be merged into a pair or tuple and reserve size
  addrsEnd_.clear();
  addrsStart_.clear();
  constexpr size_t hsize = sizeof(evf::DTHOrbitHeader_v1);
  unsigned char* nextEnd = nullptr;
  firstOrbitHeader_ = nullptr;

  for (unsigned i = 0; i < numFiles; i++) {

    bool ohThisFile = false;
    //intial orbit header was advanced over by source (first file only)
    auto nextAddr = buf + rawFile->bufferOffsets_[i];
    auto startAddr = nextAddr;//save start position of the orbit
    auto maxAddr = buf + rawFile->bufferEnds_[i];//end of stripe / file


    LogDebug("DataModeDTH::makeDataBlockView") << "blockAddr: 0x" << std::hex << (uint64_t)nextAddr << " chunkOffset: 0x"
                                               << std::hex << (uint64_t)(nextAddr - buf);

    checksumValid_ = true;
    if (!checksumError_.empty())
      checksumError_ = std::string();

    while (nextAddr < maxAddr) {
      //ensure header fits
      assert(nextAddr + hsize < maxAddr);

      auto orbitHeader = (evf::DTHOrbitHeader_v1*)(nextAddr);

      if (!orbitHeader->verifyMarker())
        throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Invalid DTH orbit marker";
      if (i == 0) {
        //get initial orbit number and find all subsequent orbits with the same nr in this file
        ohThisFile = true;
        if (!firstOrbitHeader_)
          firstOrbitHeader_ = orbitHeader;
        else {
          assert(orbitHeader->runNumber() == firstOrbitHeader_->runNumber());
          assert(orbitHeader->eventCount() == firstOrbitHeader_->eventCount());
          if (orbitHeader->orbitNumber() != firstOrbitHeader_->orbitNumber())
            //nextOrbitHeader_ = orbitHeader;
            break;
        }
      } else {
        //check that orbit headers in all files are consistent with first
        assert(firstOrbitHeader_);
        assert(orbitHeader->runNumber() == firstOrbitHeader_->runNumber());
        assert(orbitHeader->eventCount() == firstOrbitHeader_->eventCount());

        if (!ohThisFile) {
          //each file must contain at least one orbit nf of the first file
          assert(orbitHeader->orbitNumber() == firstOrbitHeader_->orbitNumber());
          ohThisFile = true;
        } else
          if (orbitHeader->orbitNumber() != firstOrbitHeader_->orbitNumber())
            break;
      }


      auto srcOrbitSize = orbitHeader->totalSize();
      nextEnd = nextAddr + srcOrbitSize;
      assert(nextEnd <= maxAddr);  //boundary check

      if (verifyChecksum_) {
        auto crc = crc32c(0U, (const uint8_t*)orbitHeader->payload(), orbitHeader->payloadSizeBytes());
        if (crc != orbitHeader->crc()) {
          checksumValid_ = false;
          if (!checksumError_.empty())
            checksumError_ += "\n";
          checksumError_ +=
            fmt::format("Found a wrong crc32c checksum in orbit header v{} run: {} orbit: {} sourceId: {} wcount: {} events: {} flags: {}. Expected {:x} but calculated {:x}",
                        orbitHeader->version(),
                        orbitHeader->runNumber(),
                        orbitHeader->orbitNumber(),
                        orbitHeader->sourceID(),
                        orbitHeader->packed_word_count(),
                        orbitHeader->eventCount(),
                        orbitHeader->flags(),
                        orbitHeader->crc(),
                        crc);
        }
      }

      addrsStart_.push_back(nextAddr + hsize);
      addrsEnd_.push_back(nextAddr + srcOrbitSize);
      nextAddr += srcOrbitSize;

    }

    //require orbit header in each file
    assert(ohThisFile);

    //report first file block size
    if (i == 0)
      dataBlockSize_ = nextEnd - nextAddr;

    //advance buffer position to next orbit
    //rawFile->bufferOffsets_[i] += nextAddr - startAddr;
    rawFile->advanceBuffer(nextAddr - startAddr, i);
  }
  //update next pointer
  //firstOrbitHeader_ = nextOrbitHeader;

  eventCached_ = false;
  blockCompleted_ = false;
  nextEventView(rawFile);
  eventCached_ = true;
}

bool DataModeDTH::nextEventView(RawInputFile*) {
  if (eventCached_)
    return true;

  blockCompleted_ = false;

  bool blockCompletedAll = !addrsEnd_.empty() ? true : false;
  bool blockCompletedAny = false;
  eventFragments_.clear();
  size_t last_eID = 0;

  for (size_t i = 0; i < addrsEnd_.size(); i++) {
    evf::DTHFragmentTrailer_v1* trailer =
        (evf::DTHFragmentTrailer_v1*)(addrsEnd_[i] - sizeof(evf::DTHFragmentTrailer_v1));

    if (!trailer->verifyMarker())
      throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Invalid DTH trailer marker";

    assert((uint8_t*)trailer >= addrsStart_[i]);

    uint64_t eID = trailer->eventID();
    eventFragments_.push_back(trailer);
    auto payload_size = trailer->payloadSizeBytes();
    if (payload_size > evf::SLR_MAX_EVENT_LEN)  //max possible by by SlinkRocket (1 MB)
      throw cms::Exception("DAQSource::DAQSourceModelsDTH")
          << "DTHFragment size " << payload_size << " larger than the SLinkRocket limit of " << evf::SLR_MAX_EVENT_LEN;

    if (i == 0) {
      nextEventID_ = eID;
      last_eID = eID;
    } else if (last_eID != nextEventID_)
      throw cms::Exception("DAQSource::DAQSourceModelsDTH") << "Inconsistent event number between fragments";

    if (trailer->flags())
      throw cms::Exception("DAQSource::DAQSourceModelsDTH")
          << "Detected error condition in DTH trailer of event " << trailer->eventID()
          << " flags: " << std::bitset<16>(trailer->flags());

    //update address array
    addrsEnd_[i] -= sizeof(evf::DTHFragmentTrailer_v1) + payload_size;

    if (addrsEnd_[i] == addrsStart_[i]) {
      blockCompletedAny = true;
    } else {
      assert(addrsEnd_[i] > addrsStart_[i]);
      blockCompletedAll = false;
    }
  }
  if (blockCompletedAny != blockCompletedAll)
    throw cms::Exception("DAQSource::DAQSourceModelsDTH")
        << "Some orbit sources have inconsistent number of event fragments.";

  if (blockCompletedAll) {
    blockCompleted_ = blockCompletedAll;
    firstOrbitHeader_ = nullptr;
    return false;
  }
  return true;
}

//striped mode functions
void DataModeDTH::makeDirectoryEntries(std::vector<std::string> const& baseDirs,
                                              std::vector<int> const& numSources,
                                              std::vector<int> const& sourceIDs,
                                              std::string const& sourceIdentifier,
                                              std::string const& runDir) {
  std::filesystem::path runDirP(runDir);
  for (auto& baseDir : baseDirs) {
    std::filesystem::path baseDirP(baseDir);
    buPaths_.emplace_back(baseDirP / runDirP);
  }
  if (!sourceIdentifier.empty()) {
    sid_pattern_ = std::regex("_" + sourceIdentifier + R"(\d+_)");

    for (auto sourceID : sourceIDs)
      buSourceStrings_.push_back("_" + sourceIdentifier + std::to_string(sourceID) + "_");

    if (baseDirs.size() != numSources.size())
      throw cms::Exception("DataModeDTH::makeDirectoryEntries") << "Number of defined directories not compatible with numSources list length";

    unsigned int sum = 0;
    for (auto numSource: numSources) {
      buNumSources_.push_back(numSource);
      sum += numSource;
    }

    if (sum != sourceIDs.size())
      throw cms::Exception("DataModeDTH::makeDirectoryEntries") << "Number of defined sources not consistent with the list of sourceIDs";
  }
}

std::pair<bool, std::vector<std::string>> DataModeDTH::defineAdditionalFiles(std::string const& primaryName,
                                                                                    bool fileListMode) const {
  //non-striped mode
  if (!buPaths_.size())
    return std::make_pair(false, std::vector<std::string>());

  std::vector<std::string> additionalFiles;

  //not touching primary file name as found by input mechanism
  auto fullpath = std::filesystem::path(primaryName);
  auto fullname = fullpath.filename();

  if (!buSourceStrings_.empty()) {
    int counter = 0;
    for (size_t i = 1; i < buPaths_.size(); i++) {
      for (size_t j = 1; j < (size_t) buNumSources_[i]; j++) {
        auto replacement = buSourceStrings_[counter];
        std::filesystem::path newPath = buPaths_[i] / std::regex_replace(primaryName, sid_pattern_, replacement);
        additionalFiles.push_back(newPath.generic_string());
        counter++;
      }
    }
  }
  else {
    for (size_t i = 1; i < buPaths_.size(); i++) {
      std::filesystem::path newPath = buPaths_[i] / fullname;
      additionalFiles.push_back(newPath.generic_string());
    }
  }
  return std::make_pair(true, additionalFiles);
}

