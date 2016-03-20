import os
import numpy as np

from numpy_allocation_tracking.decorators import assert_mem_usage_factor

import DVIDSparkServices
from DVIDSparkServices.reconutils.misc import select_channels, normalize_channels_in_place, \
                                              find_large_empty_regions, naive_membrane_predictions, \
                                              seeded_watershed

import logging
logger = logging.getLogger("unit_tests.test_misc")

def test_select_channels():
    a = np.zeros((100,200,10), dtype=np.float32)
    a[:] = np.arange(10)[None, None, :]
    
    assert select_channels(a, None) is a
    assert (select_channels(a, [2,3,5]) == np.array([2,3,5])[None, None, :]).all()
    
    combined = select_channels(a, [1,2,3,[4,5]])
    assert combined.shape == (100,200,4)
    assert (combined[..., 0] == 1).all()
    assert (combined[..., 1] == 2).all()
    assert (combined[..., 2] == 3).all()
    assert (combined[..., 3] == (4+5)).all()

def test_normalize_channels_in_place():
    a = np.zeros((100,200,3), dtype=np.float32)
    a[..., 0] = (0.2 / 2)
    a[..., 1] = (0.3 / 2)
    a[..., 2] = (0.5 / 2)

    # erase a pixel entirely
    a[50,50,:] = 0.0
    
    normalize_channels_in_place(a)
    assert (a.sum(axis=-1) == 1.0).all()

    assert a[50,50,0] == a[50,50,1] == a[50,50,2]


class TestMemoryUsage(object):

    @classmethod
    def setupClass(cls):
        cls.grayscale = cls._load_grayscale()

    @classmethod
    def _load_grayscale(cls):
        grayscale_path = os.path.split(DVIDSparkServices.__file__)[0] + '/../integration_tests/resources/grayscale-256-256-256-uint8.bin'
        with open(grayscale_path) as f:
            grayscale_bytes =  f.read()
    
        grayscale_flat = np.frombuffer(grayscale_bytes, dtype=np.uint8)
        grayscale = grayscale_flat.reshape((256, 256, 256), order='C')
        return grayscale

    def test_memory_usage(self):
        # Create a volume with an empty region
        grayscale = self.grayscale.copy()
        grayscale[0:100] = 0
        
        mask = assert_mem_usage_factor(5.1)(find_large_empty_regions)(grayscale)
        pred = assert_mem_usage_factor(4.1)(naive_membrane_predictions)(grayscale, mask)
        assert_mem_usage_factor(3.1)(normalize_channels_in_place)(pred)
        supervoxels = assert_mem_usage_factor(2.1)(seeded_watershed)(pred, mask)

if __name__ == "__main__":
    import sys
    logger.addHandler( logging.StreamHandler(sys.stdout) )
    logger.setLevel(logging.DEBUG)
    
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
