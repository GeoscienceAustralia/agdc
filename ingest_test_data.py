import os

ORTHO_DATASETS = [
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition0/L1/2005-06',
                 'LS5_TM_OTH_P51_GALPGS01-002_112_084_20050626'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition1/L1/1999-09',
                 'LS7_ETM_OTH_P51_GALPGS01-002_099_078_19990927_1'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition2/L1/2006-06',
                 'LS7_ETM_OTH_P51_GALPGS01-002_110_079_20060623'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition3/L1/2007-02',
                 'LS7_ETM_OTH_P51_GALPGS01-002_104_078_20070224'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/L1/1998-10'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/L1/1999-12',
                 'LS7_ETM_OTH_P51_GALPGS01-002_094_085_19991229_1')
    ]

NBAR_DATASETS = [
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition0/NBAR/2005-06',
                 'LS5_TM_NBAR_P54_GANBAR01-002_112_084_20050626'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition1/NBAR/1999-09',
                 'LS7_ETM_NBAR_P54_GANBAR01-002_099_078_19990927'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition2/NBAR/2006-06',
                 'LS7_ETM_NBAR_P54_GANBAR01-002_110_079_20060623'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition3/NBAR/2007-02',
                 'LS7_ETM_NBAR_P54_GANBAR01-002_104_078_20070224'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/NBAR/1998-10',
                 'LS5_TM_NBAR_P54_GANBAR01-002_101_083_19981016'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/NBAR/1999-12',
                 'LS7_ETM_NBAR_P54_GANBAR01-002_094_085_19991229'),
]

PQA_DATASETS = [
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition0/PQ/2005-06',
                 'LS5_TM_PQ_P55_GAPQ01-002_112_084_20050626'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition1/PQ/1999-09',
                 'LS7_ETM_PQ_P55_GAPQ01-002_099_078_19990927'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition2/PQ/2006-06',
                 'LS7_ETM_PQ_P55_GAPQ01-002_110_079_20060623'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition3/PQ/2007-02',
                 'LS7_ETM_PQ_P55_GAPQ01-002_104_078_20070224'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/PQ/1998-10',
                 'LS5_TM_PQ_P55_GAPQ01-002_101_083_19981016'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/PQ/1999-12',
                 'LS7_ETM_PQ_P55_GAPQ01-002_094_085_19991229'),
]

DATASETS_TO_INGEST = dict(zip(['ORTHO', 'NBAR', 'PQA'],
                              [ORTHO_DATASETS, NBAR_DATASETS, PQA_DATASETS]))

# Following directory contains tiles for above scenes from previous ingester
BENCHMARK_DIR = os.path.join(os.path.sep, 'g', 'data', 'v10',
                             'test_resources', 'benchmark', 'tiles')
# Test data for making mosaics:
MOSAIC_SOURCE_DIR='/g/data/v10/test_resources/scenes/test_mosaicing/'
MOSAIC_SOURCE_NBAR = [
    os.path.join(MOSAIC_SOURCE_DIR,
                 'NBAR//LS7_ETM_NBAR_P54_GANBAR01-002_091_077_20111224'),
    os.path.join(MOSAIC_SOURCE_DIR,
                 'NBAR//LS7_ETM_NBAR_P54_GANBAR01-002_091_078_20111224')
    ]
MOSAIC_SOURCE_ORTHO = [
    os.path.join(MOSAIC_SOURCE_DIR,
                 'L1/LS7_ETM_OTH_P51_GALPGS01-002_091_077_20111224'),
    os.path.join(MOSAIC_SOURCE_DIR,
                 'L1/LS7_ETM_OTH_P51_GALPGS01-002_091_078_20111224')
]
MOSAIC_SOURCE_PQA = [
    os.path.join(MOSAIC_SOURCE_DIR,
                 'PQA/LS7_ETM_PQ_P55_GAPQ01-002_091_077_20111224'),
    os.path.join(MOSAIC_SOURCE_DIR,
                 'PQA/LS7_ETM_PQ_P55_GAPQ01-002_091_078_20111224')
]
