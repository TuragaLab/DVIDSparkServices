"""
Simple algorithm that re-implements the Segmentor segment function

This module is a placeholder to indicate how to use the segmentation
plugin architecture.
"""
from __future__ import print_function

import os
import warnings

from DVIDSparkServices.reconutils.Segmentor import Segmentor


class precomputedpipeline(Segmentor):
    def segment(self, subvols, gray_vols=None):
        """
        Read pre-computed segmentations
        """
        import h5py
        import numpy as np

        segmentation_path = self.segmentor_config["segpath"]
        h5_dataset_key = self.segmentor_config.get(key="h5_dataset_key", default="segmentation")
        default_shape = self.segmentor_config.get(key="shape_of_blocks", default=(552, 552, 552))
        assert os.path.exists(segmentation_path), segmentation_path
        target_dtype = np.uint32

        def read_subvolume(subvolume):
            z1 = subvolume.box.z1
            y1 = subvolume.box.y1
            x1 = subvolume.box.x1
            filename = "%d_%d_%d.h5" % (z1, y1, x1)
            filepath = os.path.join(segmentation_path, filename)
            print("!!", filepath)
            try:
                with h5py.File(filepath, 'r') as h5_f:
                    seg = np.array(h5_f[h5_dataset_key])
                    print("!! good")
                    assert seg.max() <= np.iinfo(target_dtype).max, "Source segmentation has label values that overflow the dtype used to store them :("
                return seg.astype(target_dtype)
            except IOError as e:
                print(e, e.message)
                print("!! bad")
                warnings.warn("Didn't find block {} at {}".format(filename, segmentation_path))
                return np.zeros(default_shape, target_dtype)

        return subvols.map(read_subvolume, True)
