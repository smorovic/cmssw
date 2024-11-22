#ifndef IOPool_Streamer_FRDFileHeader_h
#define IOPool_Streamer_FRDFileHeader_h

#include <array>
#include <cstddef>
#include <cstdint>

//#include "IOPool/Streamer/interface/MsgTools.h"
/*
 * DTH Orbit header and event fragment trailer accompanying slink payload.
 * In this version, big-endian number format is assumed to be written
 * by DTH and requires byte swapping on low-endian platforms when converting
 * to numerical representation
 *
 * Version 1 Format defined
 * */

namespace evf {
  constexpr std::array<unsigned char, 2> DTHOrbitMarker{{0x4f, 0x48}};
  constexpr std::array<unsigned char, 2> DTHFragmentTrailerMarker{{0x46, 0x54}};
  constexpr uint32_t word_num_bytes = 16;
  constexpr uint32_t word_num_bytes_shift = 4;

  constexpr uint32_t convert(std::array<uint8_t, 4> v) {
    //LSB first
    uint32_t a = v[0], b = v[1], c = v[2], d = v[3];
    return a | (b << 8) | (c << 16) | (d << 24);
  }

  constexpr uint16_t convert(std::array<uint8_t, 2> v) {
    //LSB first
    uint16_t a = v[0], b = v[1];
    return a | (b << 8);
  }

  constexpr std::array<uint8_t, 4> convert(uint32_t i) {
    return std::array<uint8_t, 4> {{uint8_t(i & 0xff), uint8_t((i >> 8) & 0xff), uint8_t((i >> 16) & 0xff), uint8_t((i >> 24) & 0xff)}};
  }

  constexpr std::array<uint8_t, 2> convert(uint16_t i) {
    return std::array<uint8_t, 2> {{uint8_t(i & 0xff), uint8_t((i >> 8) & 0xff)}};
  }


  class DTHOrbitHeader_v1 {
  public:
    DTHOrbitHeader_v1(uint32_t source_id, uint32_t orbit_number, uint32_t run_number, uint32_t packed_word_count, uint16_t event_count, uint32_t crc, uint32_t flags)
      : //convert numbers into binary representation
        source_id_(convert(source_id)),
        orbit_number_(convert(orbit_number)),
        run_number_(convert(run_number)),
        packed_word_count_(convert(packed_word_count)),
        event_count_(convert(event_count)),
        crc32c_(convert(crc)),
        flags_(convert(flags))
      {}

    uint32_t sourceID() const { return convert(source_id_); }
    //this should be 1 but can be used for autodetection or consistency check
    uint16_t version() const { return convert(version_); }
    uint32_t orbit_number() const { return convert(orbit_number_); }
    uint32_t run_number() const { return convert(run_number_); }
    uint32_t packed_word_count() const { return convert(packed_word_count_); }
    uint64_t total_size() const { return (word_num_bytes * packed_word_count()); } //128-bit words
    uint64_t payload_size() const { return total_size() - sizeof(DTHOrbitHeader_v1); }
    uint64_t header_size() const { return sizeof(DTHOrbitHeader_v1);}
    uint16_t event_count() const { return convert(event_count_);}
    uint32_t crc() const { return convert(crc32c_);}
    uint32_t flags() const { return convert(flags_);}
    const void* payload() const { return this + sizeof(DTHOrbitHeader_v1); }
    bool verifyMarker() const {
      for (size_t i=0;i < DTHOrbitMarker.size(); i++) {
        if (marker_[i] != DTHOrbitMarker[i]) return false;
      }
      return true;
    }

    bool verifyChecksum() const;
  private:
    std::array<uint8_t, 4> source_id_;
    std::array<uint8_t, 2> version_ = {{0, 1}};
    std::array<uint8_t, 2> marker_ = DTHOrbitMarker; 
    std::array<uint8_t, 4> orbit_number_;
    std::array<uint8_t, 4> run_number_;
    std::array<uint8_t, 4> packed_word_count_;
    std::array<uint8_t, 2> reserved_ = {{0, 0}};
    std::array<uint8_t, 2> event_count_;
    std::array<uint8_t, 4> crc32c_;
    std::array<uint8_t, 4> flags_;
  };

  class DTHFragmentTrailer {
  public:
    DTHFragmentTrailer(uint32_t payload_size, uint16_t flags, uint16_t crc, uint64_t event_id)
      : payload_size_w128_(convert(payload_size >> word_num_bytes_shift)),
        flags_(convert(flags)),
        crc_(convert(crc)),
        res_and_eid_({{uint8_t((event_id & 0x0f0000000000) >> 40),
                       uint8_t((event_id & 0xff00000000) >> 32),
                       uint8_t((event_id & 0xff000000) >> 24),
                       uint8_t((event_id & 0xff0000) >> 16),
                       uint8_t((event_id & 0xff00) >> 8),
                       uint8_t(event_id & 0xff)}})
      {}

    uint64_t eventID() const {
      return (uint64_t(res_and_eid_[0]&0xf) << 40) + (uint64_t(res_and_eid_[1]) << 32) + (uint32_t(res_and_eid_[2]) << 24) + (uint32_t(res_and_eid_[3]) << 16) + (uint16_t(res_and_eid_[4]) << 8) + res_and_eid_[5];
    }
    uint32_t payload_size_w128() const { return convert(payload_size_w128_); }
    uint32_t payload_size() const { return (convert(payload_size_w128_) * word_num_bytes); }
    uint16_t flags() const { return convert(flags_); }
    uint16_t crc() const { return convert(crc_); }
    const void* payload() const { return this - payload_size(); }
    bool verifyMarker() const {
      for (size_t i=0;i < DTHFragmentTrailerMarker.size(); i++) {
        if (marker_[i] != DTHFragmentTrailerMarker[i])
          return false;
      }
      return true;
    }
  private:
    std::array<uint8_t, 4> payload_size_w128_;
    std::array<uint8_t, 2> flags_;
    std::array<uint8_t, 2> marker_ = DTHFragmentTrailerMarker;
    std::array<uint8_t, 2> crc_;
    std::array<uint8_t, 6> res_and_eid_;
  };


  class DTHFragmentTrailerView {
  public:
    DTHFragmentTrailerView(void* buf)

    : trailer_((DTHFragmentTrailer*) buf),
      payload_size_(trailer_->payload_size()),
      flags_(trailer_->flags()),
      crc_(trailer_->crc()),
      eventID_(trailer_->eventID())
    {
    }

    uint8_t* startAddress() const { return (uint8_t*)trailer_; }
    const void* payload() const { return trailer_->payload(); }
    uint32_t payload_size() const { return payload_size_; }
    uint16_t flags() const { return flags_; }
    uint16_t crc() const { return crc_; }
    uint64_t eventID() const { return eventID_; }
    bool verifyMarker() const { return trailer_ ? trailer_->verifyMarker() : false; }

  private:
    DTHFragmentTrailer* trailer_;
    uint32_t payload_size_;
    uint16_t flags_;
    uint16_t crc_;
    uint64_t eventID_;
  };
}  // namespace ecf

#endif
