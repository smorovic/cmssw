#include "EventFilter/Utilities/interface/photonMvaEstimator.h"

#include "FWCore/ParameterSet/interface/FileInPath.h"
#include "CommonTools/MVAUtils/interface/GBRForestTools.h"

#include "xgboost/c_api.h"

photonMvaEstimator::photonMvaEstimator(const edm::FileInPath& weightsfile, const edm::FileInPath& weightsFileXgb){
  //xgboost::Learner *xgb_model = xgboost::Learner::Create({&xgb_train});
  gbrForest_ = createGBRForest(weightsfile);

  XGBoosterCreate(NULL, 0, &booster_);
  //XGBoosterCreate(NULL, 0, &boosterB_);
  //XGBoosterCreate(NULL, 0, &boosterE_);
  XGBoosterLoadModel(booster_, weightsFileXgb.fullPath().c_str());
  //XGBoosterLoadModel(boosterB_, "/home/smorovic/CMSSW/CMSSW_13_3_0/src/EventFilter/Utilities/test/barrel.bin");
  //XGBoosterLoadModel(boosterE_, "/home/smorovic/CMSSW/CMSSW_13_3_0/src/EventFilter/Utilities/test/endcap.bin");
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

  std::cout << "IN ";
  for (size_t i =0;i<9;i++) std::cout << var[i] << " ";
  std::cout << "OUT(GBC):" <<  gbrForest_->GetGradBoostClassifier(var) << " "; 
  std::cout << "OUT(C):" <<  gbrForest_->GetClassifier(var) << std::endl;
    
  return gbrForest_->GetGradBoostClassifier(var);
}

double photonMvaEstimator::computeMva2(float rawEnergyIn, float r9In, float sigmaIEtaIEtaIn, float etaWidthIn, float phiWidthIn, float s4In, float etaIn, float hOvrEIn, float ecalPFIsoIn) const {
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
  XGDMatrixCreateFromMat(var, 1, 9, -1, &dmat);
  bst_ulong out_len;
  const float* out_result;
  XGBoosterPredict(booster_, dmat, 0, 0, 0, &out_len, &out_result);
  //if (fabs(etaIn) < 1.5)
  //XGBoosterPredict(boosterB_, dmat, 0, 0, 0, &out_len, &out_result);
  //else
  //  XGBoosterPredict(boosterE_, dmat, 0, 0, 0, &out_len, &out_result);
  printf("%f\n", out_result[0]);
  return out_result[0];
}
