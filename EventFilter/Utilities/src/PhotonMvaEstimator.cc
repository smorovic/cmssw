#include "EventFilter/Utilities/interface/PhotonMvaEstimator.h"

#include "FWCore/ParameterSet/interface/FileInPath.h"
#include "CommonTools/MVAUtils/interface/GBRForestTools.h"

#include <sstream>

PhotonMvaEstimator::PhotonMvaEstimator(const edm::FileInPath& weightsFile, int best_ntree_limit) {
  XGBoosterCreate(NULL, 0, &booster_);
  //std::cout << weightsFile.fullPath().c_str() << std::endl;
  XGBoosterLoadModel(booster_, weightsFile.fullPath().c_str());
  best_ntree_limit_ = best_ntree_limit;

  std::stringstream config;
  config << "{\"training\": false, \"type\": 0, \"iteration_begin\": 0, \"iteration_end\": " << best_ntree_limit_ << ", \"strict_shape\": false}";
  config_ = config.str();
}

PhotonMvaEstimator::~PhotonMvaEstimator() {
  XGBoosterFree(booster_);
}

namespace {
  enum inputIndexes {
      rawEnergy = 0,         // 0
      r9 = 1,                // 1
      sigmaIEtaIEta = 2,     // 2
      etaWidth = 3,          // 3
      phiWidth = 4,          // 4
      s4 = 5,                // 5
      eta = 6,               // 6
      hOvrE = 7,             // 7
      ecalPFIso = 8,         // 8
  };
}  // namespace

float PhotonMvaEstimator::computeMva(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const {

  float var[9];
  var[rawEnergy] = rawEnergyIn;
  var[r9]= r9In;
  var[sigmaIEtaIEta] = sigmaIEtaIEtaIn;
  var[etaWidth] = etaWidthIn;
  var[phiWidth] = phiWidthIn;
  var[s4] = s4In;
  var[eta] = etaIn;
  var[hOvrE] = hOvrEIn;
  var[ecalPFIso] = ecalPFIsoIn;

  DMatrixHandle dmat;
  XGDMatrixCreateFromMat(var, 1, 9, -999.9f, &dmat);
  uint64_t const* out_shape;
  uint64_t out_dim;
  const float* out_result = NULL;
  XGBoosterPredictFromDMatrix(booster_, dmat, config_.c_str(), &out_shape, &out_dim, &out_result);
  //bst_ulong out_len;
  //XGBoosterPredict(booster_, dmat, 0, best_ntree_limit_, 0, &out_len, &out_result); //deprecated API

  float ret = out_result[0];
  XGDMatrixFree(dmat);

  //TEST code:

  /* //printout JSON rows for a cross-check with python
  std::stringstream str2;
  str2 << "["<< out_result[0] << ", [";
  for (size_t i=0;i<8; i++)
    str2 << var[i]<< ", ";
  str2 << var[8] << "]";

  str2 << "]," << std::endl;

  std::cout << str2.str();
  */

  return ret;
}

float PhotonMvaEstimator::computeMvaTest() const {
  float var[9];
  var[0] = 116.004;
  var[1] = 1.02043;
  var[2] = 0.023201;
  var[3] = 0.00653047;
  var[4] = 0.00588042;
  var[5] = 110.456;
  var[6] = -1.76507;
  var[7] = 4.80358;
  var[8] = 0.47841;

  DMatrixHandle dmat;
  XGDMatrixCreateFromMat(var, 1, 9, -999.9f, &dmat);

  bst_ulong out_len;
  const float* out_result;

  //old XGB API
  XGBoosterPredict(booster_, dmat, 0, best_ntree_limit_, 0, &out_len, &out_result);
  XGDMatrixFree(dmat);
  std::cout << " ===TEST===   LEN: " <<  out_len << std::endl;
  printf(" ===TEST OLDAPI=== VAL[0]: %f\n", out_result[0]);
  float ret = out_result[0];

  //new XGB API
  DMatrixHandle dmat2;
  XGDMatrixCreateFromMat(var, 1, 9, -999.9f, &dmat2);
  uint64_t const* out_shape;
  uint64_t out_dim;
  float const* out_result2 = NULL;
  XGBoosterPredictFromDMatrix(booster_, dmat2, config_.c_str(), &out_shape, &out_dim, &out_result2);
  printf(" ===TEST NEWAPI=== VAL[0]: %f\n", out_result2[0]);

  return ret;
}
