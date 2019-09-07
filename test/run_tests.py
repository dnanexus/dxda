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
instance_types = ["mem1_ssd1_x4", "mem1_ssd1_x16", "mem3_ssd1_x32"]

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


def launch_and_wait(project, applet):
    # Run the workflows
    jobs=[]
    print("Launching correctness applet")

    manifest = lookup_file("manifest.json.bz2", project, "/")
    inputs = {
        "manifest" : dxpy.dxlink(manifest) #{"$dnanexus_link": manifest.get_id()}
    }

    for itype in instance_types:
        print("intance: {}".format(itype))
        job = applet.run(inputs,
                         project=project.get_id(),
                         instance_type=itype)
        jobs.append(job)
    print("executables: " + ", ".join([a.get_id() for a in jobs]))

    # Wait for completion
    wait_for_completion(jobs)
    return jobs

def extract_results(jobs):
    for j in jobs:
        desc = j.describe()
        i_type = desc["systemRequirements"]["*"]["instanceType"]
        result = desc['output']['equality']
        print("{}, {}".format(i_type, result))

def run_correctness(dx_proj):
    applet = lookup_applet("dxda_correctness", dx_proj, "/applets")
    jobs = launch_and_wait(dx_proj, applet)
    extract_results(jobs)

def main():
    argparser = argparse.ArgumentParser(description="Run benchmarks on several instance types for dxfs2")
    argparser.add_argument("--project", help="DNAnexus project",
                           default="dxfs2_test_data")
    args = argparser.parse_args()

    dx_proj = get_project(args.project)
    run_correctness(dx_proj)

if __name__ == '__main__':
    main()
