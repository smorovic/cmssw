#include "IOPool/Streamer/src/StreamerFileWriter.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"

#include <cstring>

namespace edm {
  StreamerFileWriter::StreamerFileWriter(edm::ParameterSet const& ps)
      : fileName_(ps.getUntrackedParameter<std::string>("fileName")),
        separateEvents_(ps.getUntrackedParameter<bool>("separateEvents")),
        prependInitMsg_(ps.getUntrackedParameter<bool>("prependInitMsg")),
        padding_(ps.getUntrackedParameter<unsigned int>("padding")),
        stream_writer_(new StreamerOutputFile(separateEvents_ ? (fileName_.stem().string() + ".ini") : fileName_.string(),
                                              padding_)) {}

  StreamerFileWriter::StreamerFileWriter(std::string const& fileName)
      : stream_writer_(new StreamerOutputFile(fileName)) {}

  StreamerFileWriter::~StreamerFileWriter() {}

  void StreamerFileWriter::doOutputHeader(InitMsgBuilder const& init_message) {
    //Let us turn it into a View
    InitMsgView view(init_message.startAddress());
    doOutputHeader(view);
  }

  void StreamerFileWriter::doOutputHeader(InitMsgView const& init_message) {
    //Write the Init Message to Streamer file
    if (separateEvents_ && prependInitMsg_) {
      //buffer message to write it to each event file
      iniBuf_.resize(init_message.size());
      memcpy(&iniBuf_[0], const_cast<unsigned char*>(init_message.startAddress()), init_message.headerSize());
      memcpy(&iniBuf_[0] + init_message.headerSize(), const_cast<unsigned char*>(init_message.descData()), init_message.size() - init_message.headerSize());
    }

    if (prependInitMsg_ || separateEvents_)
      stream_writer_->write(init_message);

    if (separateEvents_) stream_writer_.reset();
  }

  void StreamerFileWriter::doOutputEvent(EventMsgView const& msg) {
    //Write the Event Message to Streamer file
    if (UNLIKELY(separateEvents_)) {
      std::stringstream fname;
      fname << fileName_.stem().string() << "_event" << msg.event() << ".dat";
      std::unique_ptr<StreamerOutputFile> new_file_writer = std::make_unique<StreamerOutputFile>(fname.str(), padding_);
      if (prependInitMsg_)
        new_file_writer->writeBuf(&iniBuf_[0], iniBuf_.size());
      new_file_writer->write(msg);
    } else {
      stream_writer_->write(msg);
    }
  }

  void StreamerFileWriter::doOutputEvent(EventMsgBuilder const& msg) {
    EventMsgView eview(msg.startAddress());
    doOutputEvent(eview);
  }

  void StreamerFileWriter::fillDescription(ParameterSetDescription& desc) {
    desc.setComment("Writes events into a streamer output file.");
    desc.addUntracked<std::string>("fileName", "teststreamfile.dat")->setComment("Name of output file.");
    desc.addUntracked<bool>("separateEvents", false)->setComment("Write each event into a separate file.");
    desc.addUntracked<bool>("prependInitMsg", true)->setComment("Prepend init message to any event data file.");
    desc.addUntracked<unsigned int>("padding", 0)
        ->setComment("For testing: INIT and event block size will be rounded to this size padded with 0xff bytes.");
  }
}  //namespace edm
