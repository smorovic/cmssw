import FWCore.ParameterSet.Config as cms

def customizePrescaleSeeds(process):
    # Updated seed replacements dictionary with the new mappings
    seed_replacements = {
        'L1_DoubleMu0_Upt6_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
        'L1_DoubleMu0_Upt7_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
        'L1_DoubleMu0_Upt8_SQ_er2p0': 'L1_SingleJet46er2p5_NotBptxOR_3BX',
    }

    replaced_seeds = 0

    for moduleName in dir(process):
        module = getattr(process, moduleName)
        if hasattr(module, 'L1SeedsLogicalExpression'):
            l1SeedObj = module.L1SeedsLogicalExpression
            l1Seed = l1SeedObj.value()  # Extract the string value

            # Check and replace each possible seed
            for old_seed, new_seed in seed_replacements.items():
                if old_seed in l1Seed:
                    # Perform the replacement
                    l1Seed = l1Seed.replace(old_seed, new_seed)
                    module.L1SeedsLogicalExpression = cms.string(l1Seed)
                    replaced_seeds += 1
                    # print(f"Replaced {old_seed} with {new_seed} in {moduleName}")

    return process
