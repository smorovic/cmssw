#include "EventFilter/Utilities/interface/photonMvaEstimator.h"

#include "FWCore/ParameterSet/interface/FileInPath.h"
#include "CommonTools/MVAUtils/interface/GBRForestTools.h"

#include "xgboost/c_api.h"
#include "xgboost/learner.h"

photonMvaEstimator::photonMvaEstimator(const edm::FileInPath& weightsfile, const edm::FileInPath& weightsFileXgb, int best_ntree_limit) {
  gbrForest_ = createGBRForest(weightsfile);
  XGBoosterCreate(NULL, 0, &booster_);
  std::cout << weightsFileXgb.fullPath().c_str() << std::endl;
  XGBoosterLoadModel(booster_, weightsFileXgb.fullPath().c_str());
  best_ntree_limit_ = best_ntree_limit;
}

photonMvaEstimator::~photonMvaEstimator() {}

namespace {
  enum inputIndexes {
      rawEnergy,         // 0
      r9,                // 1
      sigmaIEtaIEta,     // 2
      etaWidth,          // 3
      phiWidth,          // 4
      s4,                // 5
      eta,               // 6
      hOvrE,             // 7
      ecalPFIso,         // 8
  };
}  // namespace

double photonMvaEstimator::computeMva(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const {
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

//  std::cout << "IN ";
//  for (size_t i =0;i<9;i++) std::cout << var[i] << " ";
//  std::cout << "OUT(GBC):" <<  gbrForest_->GetGradBoostClassifier(var) << " "; 
//  std::cout << "OUT(C):" <<  gbrForest_->GetClassifier(var) << std::endl;
    
  return gbrForest_->GetGradBoostClassifier(var);
}

double photonMvaEstimator::computeMva2(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const {
    return 0;
/*
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
    
  return gbrForest_->GetResponse(var);
*/
}

double photonMvaEstimator::computeMva3(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const {
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
  bst_ulong out_len;
  const float* out_result;
  XGBoosterPredict(booster_, dmat, 0, best_ntree_limit_, 0, &out_len, &out_result);
  XGDMatrixFree(dmat);
  return out_result[0];
}

double photonMvaEstimator::computeMva4() const {
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

  XGBoosterPredict(booster_, dmat, 0, best_ntree_limit_, 0, &out_len, &out_result);
  XGDMatrixFree(dmat);

  std::cout << " ===TEST===   LEN: " <<  out_len << std::endl;
  printf(" ===TEST=== VAL[0]: %f\n", out_result[0]);
  return out_result[0];
}
