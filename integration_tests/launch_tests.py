### Notes #####

# 1. spark-submit should be in the runtime PATH
# 2. integration tests must be in the same directory as the workflow scripts
# 3. optionally, specify a subset of tests to run via command-line args. (see below)
# 4. optionally, skip repeated initialization of dvid setup. (see below)

import sys
import subprocess
import os
import json
import time
import string
from collections import OrderedDict

import numpy as np
import vigra
from libdvid import DVIDNodeService

def run_test(test_name, plugin, test_dir, uuid1, uuid2):
    start = time.time()
    print "Starting test: ", test_name

    temp_data_dir = test_dir + "/" + test_name + "/temp_data"
    if not os.path.exists(temp_data_dir):
        os.makedirs(temp_data_dir)

    num_jobs = 8
    temp_config_json = temp_data_dir + "/config.json"
    job_command = 'spark-submit --driver-memory 2G --executor-memory 4G --master local[{num_jobs}] {test_dir}/../workflows/launchworkflow.py {plugin} -c {temp_config_json}'\
                   .format(**locals())

    print job_command
    with open(test_dir+"/"+test_name+"/config.json") as fin:
        data = fin.read()

    data = data.replace("UUID1", uuid1)
    data = data.replace("UUID2", uuid2)
    data = data.replace("DIR", test_dir)

    with open(temp_config_json, 'w') as fout:
        fout.write(data)

    try:
        correct = False
        results = subprocess.check_output(job_command, shell=True)
        with open(temp_data_dir+"/results.txt", 'w') as fout:
            fout.write(results)
        
    except subprocess.CalledProcessError as ex:
        print "BAD RETURN CODE:", ex.returncode
        # write results out anyway
        with open(temp_data_dir+"/results.txt", 'w') as fout:
            fout.write(ex.output)
    else:
        # compare results to results in output
        result_lines = results.splitlines()
        correct = True
    
        with open(test_dir+"/"+test_name+"/outputs/results.txt") as fin:
            correctoutput = fin.read()
            correct_lines = correctoutput.splitlines()
            debug1 = []
            debug2 = []
    
            # Compare only DEBUG lines, ignore everything else.
            for line in result_lines:
                if string.find(line, "DEBUG:") != -1:
                    debug1.append(line)
            for line in correct_lines:
                if string.find(line, "DEBUG:") != -1:
                    debug2.append(line)
           
            if len(debug1) != len(debug2):
                correct = False
            else:
                for iter1 in range(0, len(debug1)):
                    if debug1[iter1] != debug2[iter1]:
                        correct = False
                        break
        
        # verify output using custom python script if it exists
        if os.path.exists(test_dir+"/"+test_name+"/checkoutput.py"):
            checkoutput_cmd = ("python " + test_dir + "/" + test_name + "/checkoutput.py " + test_dir+"/"+test_name).split()

            try: 
                subprocess.check_call(checkoutput_cmd)
            except subprocess.CalledProcessError as ex:
                print "FAIL: checkoutput.py returned bad status: {}".format(ex.returncode)
                correct = False
    
        if not correct:
            print "FAILED"
        else:
            print "SUCCESS"
    
    finish = time.time()

    print "Finished test: ", test_name, " in ", finish-start, " seconds"
    return correct

def init_dvid_database(test_dir, reuse_last=False):
    uuid_file = test_dir + '/uuid-cache.txt'
    if reuse_last:
        if not os.path.exists(uuid_file):
            sys.stderr.write("Could not determine previous test uuids.\n")
            sys.exit(1)
        with open(uuid_file, 'r') as f:
            uuid1, uuid2 = f.read().split()
            return uuid1, uuid2

    print "Initializing DVID Database"
   
    os.system("gunzip -f --keep " + test_dir + "/resources/labels.bin.gz")
    os.system("gunzip -f --keep " + test_dir + "/resources/labels_comp.bin.gz")
    os.system("tar zxvf " + test_dir + "/resources/agglom.xml.tgz")
    os.system("tar zxvf " + test_dir + "/resources/voxels.ilp.gz")

    # initialize DVID datastore and call tests 
    # Curl must be available
    
    create_repo_command = "curl -X POST 127.0.0.1:8000/api/repos".split()
    
    # create first UUID
    repoinfo = subprocess.check_output(create_repo_command)
    sys.stdout.write('\n')
    data = json.loads(repoinfo)
    uuid1 = data["root"]
    
    # create second UUID
    repoinfo = subprocess.check_output(create_repo_command)
    sys.stdout.write('\n')
    data = json.loads(repoinfo)
    uuid2 = data["root"]

    # create labelblk instance for two uuids
    create_instance = 'curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d'
    typedata = "{\"typename\": \"labelblk\", \"dataname\" : \"labels\"}"
    
    create_instance1_command = (create_instance % uuid1).split()
    create_instance2_command = (create_instance % uuid2).split()
    
    create_instance1_command.append(typedata)
    create_instance2_command.append(typedata)

    subprocess.check_call(create_instance1_command)
    subprocess.check_call(create_instance2_command)
    
    # load binary label data into uuid1
    load_data1_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/labels/raw/0_1_2/512_512_512/0_0_0 --data-binary @%s/resources/labels.bin' % (uuid1, test_dir)).split()
    subprocess.check_call(load_data1_command)
    
    # load binary label data into uuid2
    load_data2_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/labels/raw/0_1_2/512_512_512/0_0_0 --data-binary @%s/resources/labels_comp.bin' % (uuid2, test_dir)).split()
    subprocess.check_call(load_data2_command)
    
    # create ROI datatype
    create_roi_command = ('curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d' % uuid1).split()
    create_roi_command.append("{\"typename\": \"roi\", \"dataname\" : \"temproi\"}")
    subprocess.check_call(create_roi_command)
    
    # load ROI
    load_roi_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/temproi/roi --data-binary @%s/resources/500roi.json' % (uuid1, test_dir)).split()
    subprocess.check_call(load_roi_command)
    
    # create synapse key value
    create_synapse_command = ('curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d' % uuid1).split()
    create_synapse_command.append("{\"typename\": \"keyvalue\", \"dataname\" : \"annotations\"}")
    subprocess.check_call(create_synapse_command)
    
    # load synapses
    load_synapse_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/annotations/key/syn --data-binary @%s/resources/synapse_small.json' % (uuid1, test_dir)).split()
    subprocess.check_call(load_synapse_command)
    
    # create grayscale data
    create_gray = 'curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d'
    typedata = "{\"typename\": \"uint8blk\", \"dataname\" : \"grayscale\"}"
    create_gray1_command = (create_gray % uuid1).split()
    create_gray1_command.append(typedata)
    subprocess.check_call(create_gray1_command)
    
    # load grayscale data
    load_gray1_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/grayscale/raw/0_1_2/256_256_256/0_0_0 --data-binary @%s/resources/grayscale-256-256-256-uint8.bin' % (uuid1, test_dir)).split()
    subprocess.check_call(load_gray1_command)

    # Grid grayscale data
    # Note that the outer border is only present on
    # the bottom Y-half.
    #
    #  +    +    +    +    +    <-- The '+' here are just a visual cue; they aren't in the data.
    #       |         |     
    #       |         |     
    #  +----+---------+----+
    #       |         |     
    #       |         |     
    #  |    |         |    |
    #  +----+---------+----+
    #  |    |         |    |
    #  |    |         |    |
    #  +----+----+----+----+
    
    # load grid grayscale data
    grid = np.zeros( (256, 256, 256), dtype=np.uint8 )
    grid[80, :, :] = 1
    grid[:, 80, :] = 1
    grid[:, :, 80] = 1
    grid[176, :, :] = 1
    grid[:, 176, :] = 1
    grid[:, :, 176] = 1

    shell = np.zeros( (256, 256, 256), dtype=np.uint8 )
    shell[0, :, :] = 1
    shell[:, 0, :] = 1
    shell[:, :, 0] = 1
    shell[-1, :, :] = 1
    shell[:, -1, :] = 1
    shell[:, :, -1] = 1

    grid[:, 128:, :] |= shell[:, 128:, :]
    grid *= 255
    grid = 255 - grid
    
    # Use a non-zero grayscale value
    grid[grid == 0] = 1
    
    a = vigra.filters.gaussianSmoothing(grid, 1.0)
    ns = DVIDNodeService('127.0.0.1:8000', str(uuid1))
    ns.create_grayscale8('grid-grayscale')
    ns.put_gray3D('grid-grayscale', a, (0,0,0))
    
    # create 256 ROI datatype
    create_roi_command = ('curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d' % uuid1).split()
    create_roi_command.append("{\"typename\": \"roi\", \"dataname\" : \"temproi256\"}")
    subprocess.check_call(create_roi_command)
    
    # load 256 ROI
    load_roi_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/temproi256/roi --data-binary @%s/resources/256roi.json' % (uuid1, test_dir)).split()
    subprocess.check_call(load_roi_command)

    # Create ROI with missing corners
    # (64px cube is missing from all 8 corners)
    create_roi_command = ('curl -X POST 127.0.0.1:8000/api/repo/%s/instance -d' % uuid1).split()
    create_roi_command.append("{\"typename\": \"roi\", \"dataname\" : \"roi-256-without-corners\"}")
    subprocess.check_call(create_roi_command)
    
    # load incomplete ROI
    load_roi_command = ('curl -X POST 127.0.0.1:8000/api/node/%s/roi-256-without-corners/roi --data-binary @%s/resources/roi-256-without-corners.json' % (uuid1, test_dir)).split()
    subprocess.check_call(load_roi_command)

    # Create diagonal ROI, useful for testing stitching
    _ = 0
    # 8x8 blocks = 256x256 px
    roi_mask_yx = np.array( [[1,_,_,_,_,_,_,1],
                             [1,1,_,_,_,_,1,1],
                             [_,1,1,_,_,1,1,_],
                             [_,_,1,1,1,1,_,_],
                             [_,_,_,1,1,_,_,_],
                             [_,_,1,1,1,1,_,_],
                             [_,1,1,_,_,1,1,_],
                             [1,1,_,_,_,_,1,1] ])

    roi_mask_zyx = np.zeros( (8,8,8) )
    roi_mask_zyx[:] = roi_mask_yx[None, :, :]
    roi_block_indexes = np.transpose( roi_mask_zyx.nonzero() )

    ns = DVIDNodeService('127.0.0.1:8000', str(uuid1))
    ns.create_roi('diagonal-256')
    ns.post_roi('diagonal-256', roi_block_indexes)

    # Save the uuids to a file for debug and/or reuse
    with open(uuid_file, 'w') as f:
        f.write(uuid1 + '\n')
        f.write(uuid2 + '\n')

    return uuid1, uuid2

def run_tests(test_dir, uuid1, uuid2, selected=[], stop_after_fail=True):
    #####  run tests ####

    tests = OrderedDict()
    tests["test_stitch_grid"] = "CreateSegmentation"
    tests["test_stitch_grid_diagonal"] = "CreateSegmentation"
    tests["test_seg"] = "CreateSegmentation"
    tests["test_Segmentor"] = "CreateSegmentation"
    tests["test_seg_iteration"] = "CreateSegmentation"
    tests["test_seg_rollback"] = "CreateSegmentation"
    tests["test_seg_with_roi"] = "CreateSegmentation"

    tests["test_cc"] = "ConnectedComponents"
    tests["test_comp"] = "EvaluateSeg"
    tests["test_graph"] = "ComputeGraph"
    tests["test_ingest"] = "IngestGrayscale"
    
    tests["test_seg_ilastik"] = "CreateSegmentation"
    tests["test_seg_simple_predict"] = "CreateSegmentation"
    tests["test_seg_ilastik_two_stage"] = "CreateSegmentation"
    tests["test_seg_wsdt"] = "CreateSegmentation"
    tests["test_seg_multicut"] = "CreateSegmentation"
    tests["test_seg_neuroproof"] = "CreateSegmentation"
    tests["test_computeprobs"] = "ComputeEdgeProbs"
    tests["test_seg_replace"] = "CreateSegmentation"
    #tests["test_tiles"] = "CreateTiles"
    #tests["test_tiles2"] = "CreateTiles2"
    #tests["test_pyramid"] = "CreatePyramid"

    # Multicut isn't included by default (yet)
    selected = set(selected) or (set(tests.keys()) - set(["test_seg_multicut"]))
    assert selected.issubset( set(tests.keys()) ), \
        "Invalid test selection.  You gave: {}".format(selected)

    results = OrderedDict()
    for test_name, workflow_name in tests.items():
        if test_name in selected:
            results[test_name] = run_test(test_name, workflow_name, test_dir, uuid1, uuid2)
            if not results[test_name] and stop_after_fail:
                break
        else:
            results[test_name] = None # Skipped
    
    print "*****************************************"
    print "*****************************************"
    print "SUMMARY OF ALL INTEGRATION TEST RESULTS: "
    print "-----------------------------------------"
    for k,v in results.items():
        print "{:.<30s} {}".format( k+' ', {True: "success", False: "FAILED", None: ""}[v] )
    print "*****************************************"
    print "*****************************************"

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--reuse-uuids", action='store_true')
    parser.add_argument("--stop-after-fail", action='store_true')
    parser.add_argument('selected_tests', nargs='*')
    args = parser.parse_args()

    # It is assumed that this script lives in the integration_tests directory
    test_dir = os.path.split(__file__)[0]
    uuid1, uuid2 = init_dvid_database(test_dir, args.reuse_uuids)
    run_tests(test_dir, uuid1, uuid2, args.selected_tests, args.stop_after_fail)
