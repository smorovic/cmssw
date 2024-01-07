#ifndef EventFilter_Utilities_photonMvaEstimator_h
#define EventFilter_Utilities_photonMvaEstimator_h

#include <vector>

#include <FWCore/Framework/interface/stream/EDProducer.h>
#include <FWCore/Framework/interface/Event.h>
#include <FWCore/ParameterSet/interface/ParameterSet.h>
#include <FWCore/Utilities/interface/InputTag.h>

#include "DataFormats/HLTReco/interface/TriggerFilterObjectWithRefs.h"
#include "DataFormats/RecoCandidate/interface/RecoEcalCandidateIsolation.h"

#include "DataFormats/EcalRecHit/interface/EcalRecHitCollections.h"
#include "RecoEcal/EgammaCoreTools/interface/EcalClusterLazyTools.h"

#include "CommonTools/MVAUtils/interface/GBRForestTools.h"
//#include "xgboost/c_api.h"
#include "xgboost/c_api.h"

class GBRForest;

namespace edm {
  class FileInPath;
}

class photonMvaEstimator {
public:
    photonMvaEstimator(const edm::FileInPath& weightsfile, const edm::FileInPath& weightsfileXgb);
  ~photonMvaEstimator();

  double computeMva(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const;
  double computeMva2(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const;
  double computeMva3(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const;

private:
  std::unique_ptr<const GBRForest> gbrForest_;
  BoosterHandle booster_;
  //BoosterHandle boosterB_;
  //BoosterHandle boosterE_;
};
#endif

