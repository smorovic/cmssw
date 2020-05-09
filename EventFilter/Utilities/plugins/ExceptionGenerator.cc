#include "ExceptionGenerator.h"

#include <iostream>
#include <typeinfo>
#include <map>
#include <sstream>
#include <sys/time.h>

#include "TRandom3.h"

#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

#include "FWCore/ServiceRegistry/interface/Service.h"
#include "EventFilter/Utilities/interface/EvFDaqDirector.h"

#include "CondFormats/DataRecord/interface/LumiTestPayloadRcd.h"
#include "CondFormats/Common/interface/LumiTestPayload.h"

#include "boost/tokenizer.hpp"

#include <cstdio>
#include <sys/types.h>
#include <csignal>

#include <chrono>

using namespace std;
using namespace std::chrono;

namespace evf{


  ExceptionGenerator::ExceptionGenerator( const edm::ParameterSet& pset) : 
    actionId_(pset.getUntrackedParameter<int>("defaultAction",-1)),
    intqualifier_(pset.getUntrackedParameter<int>("defaultQualifier",0)), 
    qualifier2_(pset.getUntrackedParameter<double>("secondQualifier",1)), 
    actionRequired_(actionId_!=-1),
    sid_(-1)
  {
    // timing destribution from (https://twiki.cern.ch/twiki/bin/viewauth/CMS/HLTCpuTimingFAQ#2011_Most_Recent_Data)
    // /castor/cern.ch/user/d/dsperka/HLT/triggerSkim_HLTPhysics_run178479_68_188.root
    // Baseline result with CMSSW_4_2_9_HLT3_hltpatch3 and /online/collisions/2011/5e33/v2.1/HLT/V9 :
    // vocms110:/store/timing_178479/outfile-178479-col1.root

    timingHisto_ = new TH1D("timingHisto_","Total time for all modules per event",100,0,1000);
    timingHisto_->SetBinContent(1,5016);
    timingHisto_->SetBinContent(2,4563);
    timingHisto_->SetBinContent(3,3298);
    timingHisto_->SetBinContent(4,1995);
    timingHisto_->SetBinContent(5,1708);
    timingHisto_->SetBinContent(6,1167);
    timingHisto_->SetBinContent(7,928);
    timingHisto_->SetBinContent(8,785);
    timingHisto_->SetBinContent(9,643);
    timingHisto_->SetBinContent(10,486);
    timingHisto_->SetBinContent(11,427);
    timingHisto_->SetBinContent(12,335);
    timingHisto_->SetBinContent(13,332);
    timingHisto_->SetBinContent(14,327);
    timingHisto_->SetBinContent(15,258);
    timingHisto_->SetBinContent(16,257);
    timingHisto_->SetBinContent(17,222);
    timingHisto_->SetBinContent(18,253);
    timingHisto_->SetBinContent(19,223);
    timingHisto_->SetBinContent(20,177);
    timingHisto_->SetBinContent(21,148);
    timingHisto_->SetBinContent(22,148);
    timingHisto_->SetBinContent(23,113);
    timingHisto_->SetBinContent(24,83);
    timingHisto_->SetBinContent(25,84);
    timingHisto_->SetBinContent(26,75);
    timingHisto_->SetBinContent(27,61);
    timingHisto_->SetBinContent(28,66);
    timingHisto_->SetBinContent(29,51);
    timingHisto_->SetBinContent(30,43);
    timingHisto_->SetBinContent(31,38);
    timingHisto_->SetBinContent(32,27);
    timingHisto_->SetBinContent(33,34);
    timingHisto_->SetBinContent(34,28);
    timingHisto_->SetBinContent(35,18);
    timingHisto_->SetBinContent(36,26);
    timingHisto_->SetBinContent(37,18);
    timingHisto_->SetBinContent(38,11);
    timingHisto_->SetBinContent(39,11);
    timingHisto_->SetBinContent(40,12);
    timingHisto_->SetBinContent(41,14);
    timingHisto_->SetBinContent(42,11);
    timingHisto_->SetBinContent(43,8);
    timingHisto_->SetBinContent(44,4);
    timingHisto_->SetBinContent(45,2);
    timingHisto_->SetBinContent(46,5);
    timingHisto_->SetBinContent(47,3);
    timingHisto_->SetBinContent(48,4);
    timingHisto_->SetBinContent(49,6);
    timingHisto_->SetBinContent(50,6);
    timingHisto_->SetBinContent(51,3);
    timingHisto_->SetBinContent(52,5);
    timingHisto_->SetBinContent(53,6);
    timingHisto_->SetBinContent(54,6);
    timingHisto_->SetBinContent(55,6);
    timingHisto_->SetBinContent(56,4);
    timingHisto_->SetBinContent(57,5);
    timingHisto_->SetBinContent(58,9);
    timingHisto_->SetBinContent(59,3);
    timingHisto_->SetBinContent(60,3);
    timingHisto_->SetBinContent(61,8);
    timingHisto_->SetBinContent(62,7);
    timingHisto_->SetBinContent(63,5);
    timingHisto_->SetBinContent(64,7);
    timingHisto_->SetBinContent(65,5);
    timingHisto_->SetBinContent(66,5);
    timingHisto_->SetBinContent(67,4);
    timingHisto_->SetBinContent(68,2);
    timingHisto_->SetBinContent(69,2);
    timingHisto_->SetBinContent(70,4);
    timingHisto_->SetBinContent(71,5);
    timingHisto_->SetBinContent(72,4);
    timingHisto_->SetBinContent(73,5);
    timingHisto_->SetBinContent(74,3);
    timingHisto_->SetBinContent(75,5);
    timingHisto_->SetBinContent(76,3);
    timingHisto_->SetBinContent(77,9);
    timingHisto_->SetBinContent(78,2);
    timingHisto_->SetBinContent(79,2);
    timingHisto_->SetBinContent(80,5);
    timingHisto_->SetBinContent(81,5);
    timingHisto_->SetBinContent(82,5);
    timingHisto_->SetBinContent(83,5);
    timingHisto_->SetBinContent(84,4);
    timingHisto_->SetBinContent(85,4);
    timingHisto_->SetBinContent(86,9);
    timingHisto_->SetBinContent(87,5);
    timingHisto_->SetBinContent(88,4);
    timingHisto_->SetBinContent(89,4);
    timingHisto_->SetBinContent(90,5);
    timingHisto_->SetBinContent(91,3);
    timingHisto_->SetBinContent(92,3);
    timingHisto_->SetBinContent(93,3);
    timingHisto_->SetBinContent(94,7);
    timingHisto_->SetBinContent(95,5);
    timingHisto_->SetBinContent(96,6);
    timingHisto_->SetBinContent(97,2);
    timingHisto_->SetBinContent(98,3);
    timingHisto_->SetBinContent(99,5);
    timingHisto_->SetBinContent(101,147);
    timingHisto_->SetEntries(24934);
    
    curl_global_init(CURL_GLOBAL_ALL);



    uname(&uts_);
  }

  void ExceptionGenerator::beginStream(edm::StreamID sid) {
    sid_=sid.value();
  }

  void ExceptionGenerator::beginRun(const edm::Run& r, const edm::EventSetup& iSetup)
  {

    gettimeofday(&tv_start_,nullptr);
    std::lock_guard<std::mutex> guard(stream_mutex_);
    std::stringstream ss;
    ss <<"http://es-cdaq.cms:9200/test_conddb/doc";
    url_ = ss.str();

    if (actionId_==16 || actionId_==17) {
      edm::ESHandle<cond::LumiTestPayload> ps;
      iSetup.get<LumiTestPayloadRcd>().get(ps);
    }

  }

  void ExceptionGenerator::beginLuminosityBlock(edm::LuminosityBlock const &lb, edm::EventSetup const &es)
  {
    if (actionId_==17)
      getLumiTestPayload(nullptr,&lb,&es,false);
    if (actionId_==18)
      getLumiTestPayload(nullptr,&lb,&es,true);
  }

  void ExceptionGenerator::getLumiTestPayload(edm::Event const* e, edm::LuminosityBlock const *lb,  edm::EventSetup const *es, bool writeToFile) {


    //check only once per LS
    unsigned int ls;
    unsigned long eid;
    unsigned long run;
    if (e) {
      eid = e->id().event();
      ls = e->id().luminosityBlock();
    }
    else if (lb) {
      eid = 0;
      ls = lb->luminosityBlock();
      run = lb->run();
    }
    else {
      run = eid = ls = 0;
      assert(!(e==nullptr && lb==nullptr));
    }

    {
      std::lock_guard<std::mutex> guard(stream_mutex_);
      if (ls_handled_.find(ls)!=ls_handled_.end()) return;
      else ls_handled_[ls]=true;
    }

    //milliseconds ms1 = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    //long pre = ms1.count();

    edm::eventsetup::EventSetupRecordKey recordKey(edm::eventsetup::EventSetupRecordKey::TypeTag::findType("LumiTestPayloadRcd"));
    if( recordKey.type() == edm::eventsetup::EventSetupRecordKey::TypeTag()) {
      //record not found
      edm::LogError("ExceptionGenerator") << "Record \"LumiTestPayloadRcd"<<"\" does not exist ";
    }
    edm::ESHandle<cond::LumiTestPayload> ps;
    es->get<LumiTestPayloadRcd>().get(ps);
    const cond::LumiTestPayload* payload=ps.product();
    auto m_id = payload->m_id;
    auto pls = (m_id & 0xffffffff);
    
    edm::LogWarning("ExceptionGenerator") << "Event "<< eid << " Lumi "<<ls <<" payload_ls" << pls;

    //long delta_ls = (long)ls - (long)payload->m_id;
    long delta_ls = (long)ls - (long)pls;

    //unix timestamp (for example)
    //milliseconds ms2 = duration_cast< milliseconds >(system_clock::now().time_since_epoch());
    //long post = ms2.count();

    //auto initial_time = testHandle->getUnixTime();

    //auto dt1 = ms1-initial_time;
    //auto dt2 = ms2-initial_time;

    timeval td;
    gettimeofday(&td,nullptr);

    std::stringstream ss;
    //ss <<"{\"doc_type\":\"condlatency\",\"MergingTimePerFile\":" << dt1 << ",\"MergingTime\":"<<dt2<<",\"ls\":"<< ls <<"}"; // todo: ,host;>>...
    ss <<"{\"host\":\""<< uts_.nodename << "\",\"pid\":"<< getpid() <<",\"run\":" <<  run <<",\"delta_ls\":" << delta_ls << ",\"ls\":"<< ls << ",\"doc_type\":\"condtest\""
       << ",\"date\":" << ((unsigned long long)(td.tv_sec)*1000 + (unsigned long long)(td.tv_usec/1000))  <<",\"m_id\":\""<< m_id << "\"}"; // todo: ,host;>>...
    std::string data = ss.str();

    /* HTTP PUT please */ 
    if (writeToFile) {
      std::stringstream fps;
      fps << edm::Service<evf::EvFDaqDirector>()->baseRunDir() << "/mon/"
          << "procmon_ls" << std::setfill('0') << std::setw(4) << ls << "_pid" << std::setfill('0') << std::setw(5) << getpid()
          << "_tid" << sid_ << ".jsn";
      std::ofstream outputFile;
      outputFile.open(fps.str().c_str());
      outputFile << ss.str();
      outputFile.close();
    }
    else { //write to elastic directly
      CURL * curl = curl_easy_init();
  //    curl_easy_setopt(curl_, CURLOPT_UPLOAD, 1L); //?
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
      curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data.size());


      struct curl_slist *chunk = NULL;
      chunk = curl_slist_append(chunk, "content-type:application/json");
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

      CURLcode res = curl_easy_perform(curl);
      /* Check for errors */ 
      if(res != CURLE_OK)
              fprintf(stderr, "curl_easy_perform() failed: %s\n",
                              curl_easy_strerror(res));

      curl_easy_cleanup(curl);
    }
  }


  void __attribute__((optimize("O0"))) ExceptionGenerator::analyze(const edm::Event & e, const edm::EventSetup& c)
    {
      float dummy = 0.;
      unsigned int iterations = 0;
      if(actionRequired_) 
	{
	  int *pi = nullptr;//null-pointer used with actionId_ 8 and 12 to intentionally cause segfault
	  int ind = 0; 
	  int step = 1; 
	  switch(actionId_)
	    {
	    case 0:
	      ::usleep(intqualifier_*1000);
	      break;
	    case 1:
	      ::sleep(0xFFFFFFF);
	      break;
	    case 2:
	      throw cms::Exception(qualifier_) << "This exception was generated by the ExceptionGenerator";
	      break;
	    case 3:
	      exit(-1);
	      break;
	    case 4:
	      abort();
	      break;
	    case 5:
	      throw qualifier_; 
	      break;
	    case 6:
	      while(true){ind+=step; if(ind>1000000) step = -1; if(ind==0) step = 1;}
	      break;
	    case 7:
	      edm::LogError("TestErrorMessage") << qualifier_;
	      break;
	    case 8:
	      *pi=0;//intentionally caused segfault by assigning null pointer (this produces static-checker warning)
	      break;
	    case 9:
	      for(unsigned int j=0; j<intqualifier_*1000*100;j++){
		dummy += sqrt(log(float(j+1)))/float(j*j);
	      }
	      break;
            case 10:
              iterations = 100*static_cast<unsigned int>(
                timingHisto_->GetRandom() * intqualifier_*17. + 0.5
              );
	      for(unsigned int j=0; j<iterations;j++){
		dummy += sqrt(log(float(j+1)))/float(j*j);
	      }
              break;
            case 11:
	      { 
                iterations = static_cast<unsigned int>(
                  timingHisto_->GetRandom() * intqualifier_*12. + 0.5
                );
                TRandom3 random(iterations);
                const size_t dataSize = 32*500; // 124kB
                std::vector<double> data(dataSize);
                random.RndmArray(dataSize, &data[0]);
              
	        for(unsigned int j=0; j<iterations;j++){
                  const size_t index = static_cast<size_t>(random.Rndm() * dataSize + 0.5);
                  const double value = data[index];
		  dummy += sqrt(log(value+1))/(value*value);
                  if ( random.Rndm() < 0.1 )
                    data[index] = dummy;
	        }
	      }
              break;
	    case 12:
	      {
		timeval tv_now;
	        gettimeofday(&tv_now,nullptr);
		if ((unsigned)(tv_now.tv_sec-tv_start_.tv_sec)>intqualifier_)
		  *pi=0;//intentionally caused segfault by assigning null pointer (this produces static-checker warning)
	      }
	      break;
	    case 13:
              {
	        void *vp = malloc(1024);
	        memset((char *)vp - 32, 0, 1024);
	        free(vp);
              }
	      break;
	    case 14:
              {
                float mean = 60.; // timingHisto_->GetMean();
                float scale = intqualifier_ / mean;
                float off = intqualifier_ * (1. - qualifier2_);
                scale  = scale*qualifier2_; // scale factor (1 default)
                iterations = static_cast<unsigned int>(max(1.,off + timingHisto_->GetRandom() * scale));
                //std::cout << " off " << off << " scale " << scale << " " << iterations << std::endl;
                ::usleep(iterations*1000);
              }
              break;
	    case 15:
              {
                float mean = 60.; // timingHisto_->GetMean();
                float scale = intqualifier_ / mean;
                float off = intqualifier_ * (1. - qualifier2_);
                scale  = scale*qualifier2_; // scale factor (1 default)
                iterations = static_cast<unsigned int>(max(1.,off + timingHisto_->GetRandom() * scale));
                iterations *= 100000;
	        for(unsigned int j=0; j<iterations;j++){
		  dummy += sqrt(log(float(j+1)))/float(j*j);
	        }
              }
              break;
            case 16:
              {
                getLumiTestPayload(&e,nullptr,&c,false); 
                break;
              }
            default:
              break;
	    }

	}
    }
    
    void ExceptionGenerator::endLuminosityBlock(edm::LuminosityBlock const &lb, edm::EventSetup const &es)
    {

    }
    

} // end namespace evf
