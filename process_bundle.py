#!/usr/bin/env python3

import click
import os
import tarfile
import pprint

import json
import click_pathlib

from pathlib import Path
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

def convert_memory_units(memory_string):
    if memory_string.endswith("Mi"):
        return int(memory_string[:-2])
    elif memory_string.endswith("M"):
        return int(memory_string[:-1]) * 1000000/1024**2
    elif memory_string.endswith("Gi"):
        return int(memory_string[:-2]) * 1024
    elif memory_string.endswith("G"):
        return int(memory_string[:-1]) * 1000**3/1024**3
    elif memory_string.endswith("Ki"):
        return int(memory_string[:-2]) / 1024
    elif memory_string.endswith("K"):
        return int(memory_string[:-1]) * 1000/1024**2
    else:
        assert False, f"unsupported units for memory limit: {memory_string}"


def convert_cpu_units(cpu_string):
    if cpu_string.endswith("m"):
        return int(cpu_string[:-1]) / 1000
    else:
        return int(cpu_string)

def convert_to_json(path):
    if path.suffix != ".yaml" and path.suffix != ".yml":
        return
    path_json = path.with_suffix(".json")

    print("      converting to JSON...")
    data = load(path.read_bytes() , Loader=Loader)
    path_json.write_text(json.dumps(data), encoding='utf8')


@click.group()
def cli():
    pass

@cli.command()
@click.argument('bundle-file', nargs=1, required=True, type=click_pathlib.Path(exists=True,dir_okay=False,file_okay=True,readable=True))
@click.option('--json-convert/--no-json-convert', default=True)
def unpack(bundle_file, json_convert):
    click.echo(f'Unpacking the diagnostics bundle `{bundle_file}`')

    outer_tarfile = tarfile.open(bundle_file, 'r:*')
    assert bundle_file.suffixes == ['.tar', '.gz']
    output_dir = Path(str(bundle_file)[:-7])

    for outer_entry in outer_tarfile.getnames():
        click.echo(f"  - unpacking inner tarball {outer_entry}")
        filename = os.path.basename(outer_entry)

        # Changed bundle format?
        assert filename.endswith('.tar.gz')

        inner_dirname = output_dir / filename[:-7]

        outer_tarball = outer_tarfile.extractfile(outer_entry)
        inner_tarfile = tarfile.open(fileobj=outer_tarball, mode='r:*')
        for inner_entry in inner_tarfile:
            click.echo(f"    - unpacking file {inner_entry.name}")
            inner_tarfile.extract(inner_entry.name, inner_dirname)
            if json_convert:
                convert_to_json(inner_dirname / inner_entry.name)


@cli.command()
@click.option('--bundle-dir', required=True, type=click_pathlib.Path(exists=True,dir_okay=True,file_okay=False,readable=True))
def resources(bundle_dir):
    click.echo(f'Analysing the resource usage of the given cluster from path {bundle_dir}')

    nodes_yaml_path = bundle_dir / "cluster-data/api-resources/nodes.yaml"
    nodes_data = load(nodes_yaml_path.read_bytes() , Loader=Loader)
    nodes_resources = {}
    for node_data in nodes_data["items"]:
        node_name = node_data["metadata"]["name"]
        assert node_name not in nodes_resources
        nodes_resources[node_name] = {
            "cpu_limit": 0,
            "memory_limit": 0,
            "cpu_request": 0,
            "memory_request": 0,
            "pool": node_data["metadata"]["labels"]["konvoy.mesosphere.com/node_pool"]
        }
        cpu_alloc_str = node_data["status"]["allocatable"]["cpu"]
        nodes_resources[node_name]["cpu_allocatable"] = convert_cpu_units(cpu_alloc_str)
        mem_alloc_str = node_data["status"]["allocatable"]["memory"]
        nodes_resources[node_name]["memory_allocatable"] = convert_memory_units(mem_alloc_str)

    pods_resources = {}
    unallocated_pods = []
    pods_yaml_path = bundle_dir / "cluster-data/api-resources/pods.yaml"
    pods_data = load(pods_yaml_path.read_bytes() , Loader=Loader)
    for pod_data in pods_data["items"]:
        pod_name_namespaced = "{}/{}".format(
            pod_data["metadata"]["namespace"],
            pod_data["metadata"]["name"],
        )
        assert pod_name_namespaced not in pods_resources
        if "nodeName" in pod_data["spec"]:
            node_name = pod_data["spec"]["nodeName"]
        else:
            node_name = "unallocated"
            unallocated_pods.append(pod_name_namespaced)
        cpu_request_total = 0
        mem_request_total = 0
        cpu_limit_total = 0
        mem_limit_total = 0
        for container in pod_data["spec"]["containers"]:
            if "limits" in container["resources"]:
                if "memory" in container["resources"]["limits"]:
                    memlimit = container["resources"]["limits"]["memory"]
                    mem_limit_total += convert_memory_units(memlimit)
                if "cpu" in container["resources"]["limits"]:
                    cpulimit = container["resources"]["limits"]["cpu"]
                    cpu_limit_total += convert_cpu_units(cpulimit)
            if "requests" in container["resources"]:
                if "memory" in container["resources"]["requests"]:
                    memrequest = container["resources"]["requests"]["memory"]
                    mem_request_total += convert_memory_units(memrequest)
                if "cpu" in container["resources"]["requests"]:
                    cpurequest = container["resources"]["requests"]["cpu"]
                    cpu_request_total += convert_cpu_units(cpurequest)
            else:
                # Default resource limit is 1vCPU, 512MiB
                cpu_limit_total += 1
                mem_limit_total += 512
                continue

        pods_resources[pod_name_namespaced] = {
            "cpu_limit": cpu_limit_total,
            "memory_limit": mem_limit_total,
            "cpu_request": cpu_request_total,
            "memory_request": mem_request_total,
            "node_name": node_name,
        }

    if unallocated_pods:
        click.echo("Unallocated pods found:")
        for pod in unallocated_pods:
            click.echo(f" - `{pod}`")
            pprint.pprint(pods_resources[pod])

    for pod in pods_resources:
        pods_node = pods_resources[pod]["node_name"]
        if pods_node == "unallocated":
            continue
        assert pods_node in nodes_resources
        for i in ["cpu_limit", "memory_limit", "cpu_request", "memory_request"]:
            nodes_resources[pods_node][i] += pods_resources[pod][i]

    click.echo("Node resources:")
    pprint.pprint(nodes_resources)


if __name__ == '__main__':
    cli()
