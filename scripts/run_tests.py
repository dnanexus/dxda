#!/usr/bin/env python
import argparse
from collections import namedtuple
import dxpy
import json
import pprint
import os
import sys
import subprocess
from typing import Callable, Iterator, Union, Optional, List
from termcolor import colored, cprint
import time
from dxpy.exceptions import DXJobFailureError

# The list of instance types to test on. We don't want too many, because it will be expensive.
# We are trying to take a representative from small, medium, and large instances.
aws_regular_instances = {
    "small" : ["mem1_ssd1_v2_x4"],
    "large" : ["mem1_ssd1_v2_x4", "mem1_ssd1_v2_x16", "mem3_ssd1_v2_x32"]
}

# AWS instances
aws_large_instances = {
    # c5d.18xlarge with 5.6 TiB of local storage and 25 Gbps networking.
    # "small" : ["mem1_ssd2_v2_x72"],

    # c5d.9xlarge  with 2.8 TiB of local storage and 10 Gbps networking.
    "small" : ["mem1_ssd2_v2_x36"],

    # dx_i3en.6xlarge with 15 TiB of local storage, and 25 Gbps networking
    "large" : ["mem3_ssd3_x24"]
}

azure_regular_instances = {
    "small" : ["azure:mem1_ssd1_x4"],
    "large" : ["azure:mem1_ssd1_x4", "azure:mem1_ssd1_x16", "azure:mem3_ssd1_x16"]
}
azure_large_instances = {
    "small" : ["azure:mem3_ssd1_x16"],
    "large" : ["azure:mem3_ssd1_x16"]
}

def lookup_applet(name, project, folder):
    applets = dxpy.bindings.search.find_data_objects(classname="applet",
                                                   name= name,
                                                   folder= folder,
                                                   project= project.get_id(),
                                                   limit= 1)
    objs = [item for item in applets]
    if len(objs) == 0:
        raise RuntimeError("applet {} not found in folder {}".format(name, folder))
    if len(objs) == 1:
        oid = objs[0]['id']
        return dxpy.DXApplet(project=project.get_id(), dxid=oid)
    raise RuntimeError("sanity")

def lookup_file(name, project, folder):
    files = dxpy.bindings.search.find_data_objects(classname="file",
                                                   name= name,
                                                   folder= folder,
                                                   project= project.get_id(),
                                                   limit= 1)
    objs = [item for item in files]
    if len(objs) == 0:
        raise RuntimeError("file {} not found in folder {}".format(name, folder))
    if len(objs) == 1:
        oid = objs[0]['id']
        return dxpy.DXFile(project=project.get_id(), dxid=oid)
    raise RuntimeError("sanity")

def wait_for_completion(jobs):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for job in jobs:
            try:
                job.wait_on_done()
            except DXJobFailureError:
                raise RuntimeError("Executable {} failed".format(job.get_id()))
    finally:
        noise.kill()
    print("done")

def get_project(project_name):
    '''Try to find the project with the given name or id.'''

    # First, see if the project is a project-id.
    try:
        project = dxpy.DXProject(project_name)
        return project
    except dxpy.DXError:
        pass

    project = dxpy.find_projects(name=project_name, name_mode='glob', return_handler=True, level="VIEW")
    project = [p for p in project]
    if len(project) == 0:
        print('Did not find project {0}'.format(project_name), file=sys.stderr)
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name))


def launch_and_wait(project, applet, manifest, instance_types, ssh_flag):
    print("instance types={}".format(instance_types))
    # Run the applets
    jobs=[]
    inputs = {
        "manifest" : dxpy.dxlink(manifest),
        "gc_info" : True
    }

    run_kwargs = {}
    if ssh_flag:
        run_kwargs = {
            "allow_ssh" : [ "*" ]
        }

    for itype in instance_types:
        print("intance: {}".format(itype))
        job = applet.run(inputs,
                         project=project.get_id(),
                         instance_type=itype,
                         **run_kwargs)
        jobs.append(job)
    print("executables: " + ", ".join([a.get_id() for a in jobs]))

    # Wait for completion
    wait_for_completion(jobs)
    return jobs


def run_correctness(dx_proj, instance_types, ssh_flag):
    applet = lookup_applet("dxda_correctness", dx_proj, "/applets")
    manifest = lookup_file("correctness.manifest.json.bz2", dx_proj, "/dxda")
    jobs = launch_and_wait(dx_proj, applet, manifest, instance_types, ssh_flag)

    # extract results
    for j in jobs:
        desc = j.describe()
        i_type = desc["systemRequirements"]["*"]["instanceType"]
        result = desc['output']['equality']
        print("{}, {}".format(i_type, result))



# extract results
def extract_benchmark_results(jobs):
    for j in jobs:
        desc = j.describe()
        i_type = desc["systemRequirements"]["*"]["instanceType"]
        result = desc['output']['runtime']
        print("{}, {}".format(i_type, result))

def run_benchmark(dx_proj, instance_types, ssh_flag):
    applet = lookup_applet("dxda_benchmark", dx_proj, "/applets")
    manifest = lookup_file("benchmark.manifest.json.bz2", dx_proj, "/dxda")
    jobs = launch_and_wait(dx_proj, applet, manifest, instance_types, ssh_flag)
    extract_benchmark_results(jobs)

def run_large_data(dx_proj, instance_types, ssh_flag):
    applet = lookup_applet("dxda_benchmark", dx_proj, "/applets")
    manifest = lookup_file("ukbb_cram_2TB.manifest.json.bz2", dx_proj, "/dxda")
    jobs = launch_and_wait(dx_proj, applet, manifest, instance_types, ssh_flag)
    extract_benchmark_results(jobs)

def run_one_big_file(dx_proj, instance_types, ssh_flag):
    applet = lookup_applet("dxda_benchmark", dx_proj, "/applets")
    manifest = lookup_file("one_64GiB_file.manifest.json.bz2", dx_proj, "/dxda")
    jobs = launch_and_wait(dx_proj, applet, manifest, instance_types, ssh_flag)
    extract_benchmark_results(jobs)


def is_large(test_name):
    return ((test_name == "large_data") or
            (test_name == "one_big_file"))

def choose_instance_scale(region, test_name):
    if region.startswith("aws:"):
        if is_large(test_name):
            return aws_large_instances
        else:
            return aws_regular_instances
    if region.startswith("azure"):
        if is_large(test_name):
            return azure_large_instances
        else:
            return azure_regular_instances
    raise Exception("unknown region {}".format(region))

def main():
    argparser = argparse.ArgumentParser(description="Run benchmarks on several instance types for dxda")
    argparser.add_argument("--project", help="DNAnexus project",
                           default="dxfuse_test_data")
    argparser.add_argument("--ssh", help="allow ssh access to running jobs",
                           action="store_true", default=False)
    argparser.add_argument("--test", help="which testing suite to run [bench, correct, large_data, one_big_file]")
    argparser.add_argument("--size", help="how large should the test be? [small, large]",
                           default="small")
    argparser.add_argument("--verbose", help="run the tests in verbose mode",
                           action='store_true', default=False)
    args = argparser.parse_args()
    dx_proj = get_project(args.project)

    # figure out which region we are operating in
    region = dx_proj.describe()["region"]
    scale = choose_instance_scale(region, args.test)

    if args.size in scale.keys():
        instance_types = scale[args.size]
    else:
        print("Unknown size value {}".format(args.scale))
        exit(1)

    if args.test is None:
        print("Test not specified")
        exit(1)
    if args.test.startswith("bench"):
        run_correctness(dx_proj, instance_types, args.ssh)
    elif args.test.startswith("correct"):
        run_benchmark(dx_proj, instance_types, args.ssh)
    elif args.test.startswith("large_data"):
        run_large_data(dx_proj, instance_types, args.ssh)
    elif args.test.startswith("one_big_file"):
        run_one_big_file(dx_proj, instance_types, args.ssh)
    else:
        print("Unknown test {}".format(args.test))
        exit(1)

if __name__ == '__main__':
    main()
