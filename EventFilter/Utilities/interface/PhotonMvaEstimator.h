#ifndef EventFilter_Utilities_PhotonMvaEstimator_h
#define EventFilter_Utilities_PhotonMvaEstimator_h

#include <iostream>
#include "xgboost/c_api.h"


namespace edm {
  class FileInPath;
}

class PhotonMvaEstimator {
public:
    PhotonMvaEstimator(const edm::FileInPath& weightsFile, int best_ntree_limit);
  ~PhotonMvaEstimator();

  float computeMva(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const;
  float computeMvaTest() const;

private:
  BoosterHandle booster_;
  int best_ntree_limit_ = -1;
  std::string config_;
};

#endif

