from __future__ import print_function

import importlib
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)

DEBUG_LOGGING = True
if DEBUG_LOGGING:
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def run(workflow, config_file):
    print("At", time.time(), "starting", workflow, "for", config_file)
    # import plugin and grab class
    # assume plugin name and class name are the same
    module_name = "workflows." + workflow
    workflow_mod = importlib.import_module(module_name)
    workflow_cls = getattr(workflow_mod, workflow)
    workflow_inst = workflow_cls(config_file)
    workflow_inst.execute()
    workflow_inst.sc.stop()  # this is a SparkContext, from workflow.Workflow._init_spark
    print("At", time.time(), "exiting", workflow, "for", config_file)


def make_label_instance(hostname, port, uuid, name):
    # 1. initialize labelblk, not versioned
    # 2. initialize labelvol, not versioned
    # 3. sync one with other
    import json
    import requests
    def make_label_instance_and_check(data):
        url = "http://{h}:{p}/api/repo/{u}/instance"\
            .format(h=hostname, p=port, u=uuid)
        r = requests.post(url, data)
        try:
            assert r.ok, (r.url, r.text)
        except:
            if "already exists" not in r.text:
                raise
        return r

    name_vol = name + "-vol"
    name_blk = name
    data = json.dumps(dict(typename="labelblk", dataname=name_blk, versioned="false"))
    make_label_instance_and_check(data)
    data = json.dumps(dict(typename="labelvol", dataname=name_vol, versioned="false"))
    make_label_instance_and_check(data)
    url = "http://{h}:{p}/api/node/{u}/{n}/sync"\
        .format(h=hostname, p=port, u=uuid, n=name_blk)
    data = json.dumps(dict(sync=name_vol))
    r = requests.post(url, data)
    assert r.ok, r.text
    url = "http://{h}:{p}/api/node/{u}/{n}/sync"\
        .format(h=hostname, p=port, u=uuid, n=name_vol)
    data = json.dumps(dict(sync=name_blk))
    r = requests.post(url, data)
    assert r.ok, r.text


dvid_hostname = "slowpoke3"
dvid_port = 32788
uuid = "341635bc8c864fa5acbaf4558122c0d5"

# make_label_instance(dvid_hostname, dvid_port, uuid, "groundtruth-eroded7_z_gte_5024")
# 1. run ConnectedComponents with config
# run("ConnectedComponents",
#     "/groups/turaga/home/grisaitisw/src/DVIDSparkServices/run_stuff/configs/groundtruth/connected_components.json")
# 2. run pyramid
# run("CreatePyramid",
#     "/groups/turaga/home/grisaitisw/src/DVIDSparkServices/run_stuff/configs/groundtruth/create_pyramid.json")


cropped_roi_names = ["seven_column_eroded7_z_gte_5024"]
roi_of_chunked_segs = "seven_column_eroded7_z_gte_5024"


class Model(object):
    def __init__(self, name, iteration):
        self.name = name
        self.iteration = iteration
        self.outputs_path = "/nrs/turaga/grisaitisw/affinities/{m}/{i}".format(m=self.name, i=self.iteration)


class BlockedSegmentation(object):
    def __init__(self, name, model, merge_function, roi):
        self.name = name
        self.model = model
        self.merge_function = merge_function
        self.roi = roi
        path = os.path.join(model.outputs_path, name)
        assert os.path.exists(path), path
        self.path = path


class StitchedSegmentation(object):
    def __init__(self, blocked_segmentation, threshold):
        self.model = blocked_segmentation.model
        self.merge_function = blocked_segmentation.merge_function
        self.threshold = threshold

    @property
    def config_dir(self):
        return "/groups/turaga/home/grisaitisw/src/dvid-spark-testing/" \
               "DVIDSparkServices_46faeeb/run_stuff/configs/{m}_{i}-{mf}/{t:5.3f}" \
            .format(m=self.model.name, i=self.model.iteration, mf=self.merge_function, t=self.threshold)

    def stitch(self):
        config_file_path = os.path.join(self.config_dir, "01_create_segmentation.json")
        assert os.path.exists(config_file_path), config_file_path
        label_name = \
            "{n}-{i}-{t:5.3f}-{mf}-aggressive-eroded7_z_gte_5024" \
                .format(n=self.model.name, i=self.model.iteration, t=self.threshold, mf=self.merge_function)
        make_label_instance(dvid_hostname, dvid_port, uuid, label_name)
        run("CreateSegmentation", config_file_path)

    def make_pyramid(self):
        config_file_path = os.path.join(self.config_dir, "03_create_pyramid.json")
        assert os.path.exists(config_file_path), config_file_path
        run("CreatePyramid", config_file_path)

    def evaluate(self):
        config_file_path = os.path.join(self.config_dir, "04_evaluate_seg.json")
        assert os.path.exists(config_file_path), config_file_path
        run("EvaluateSeg", config_file_path)


run_1208_6_400000 = Model("run_1208_6", 400000)
run_1229_5_160000 = Model("run_1229_5", 160000)

model_segs = [
    BlockedSegmentation("fib25-e402c09-2017.01.20-17.37.16.segmentations.5.seven_column_eroded7_z_gte_5024",
                        run_1208_6_400000, "50th", "seven_column_eroded7_z_gte_5024"),
    BlockedSegmentation("fib25-e402c09-2017.01.20-17.37.16.segmentations.6.85th.eroded7_z_gte_5024",
                        run_1208_6_400000, "85th", "seven_column_eroded7_z_gte_5024"),
    BlockedSegmentation("fib25-e402c09-2017.01.20-17.37.16.segmentations.7.50th.minsize100.eroded7_z_gte_5024",
                        run_1208_6_400000, "50th_min100", "seven_column_eroded7_z_gte_5024"),
    BlockedSegmentation("fib25-e402c09-2017.02.14-06.37.10.segmentations.2.seven_column_eroded7_z_gte_5024",
                        run_1229_5_160000, "50th", "seven_column_eroded7_z_gte_5024"),
    BlockedSegmentation("fib25-e402c09-2017.02.14-06.37.10.segmentations.3.85th.eroded7_z_gte_5024",
                        run_1229_5_160000, "85th", "seven_column_eroded7_z_gte_5024"),
]

thresholds = [0.90, 0.95, 0.80, 0.96, 0.85, 0.75]  # for 50th %ile mergers
# thresholds = [0.75, 0.80, 0.85, 0.90, 0.92, 0.87, 0.82, 0.77]  # for 85th %ile mergers
segmentations = [
    StitchedSegmentation(ms, t)
    for ms in model_segs
    for t in thresholds
]


for segmentation in segmentations:
    segmentation.stitch()

for segmentation in segmentations:
    segmentation.make_pyramid()

for segmentation in segmentations:
    segmentation.evaluate()
