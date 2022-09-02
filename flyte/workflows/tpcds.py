import paramiko
from flytekit import task, workflow, Resources
import subprocess
import sys
import os
import time
import random
import glob
import re
from paramiko import SSHClient
from scp import SCPClient
from pyhive import hive
from flyte.workflows.externaltable import externaltables


MAX_BACKOFF_UNIT = 60
MIN_BACKOFF_UNIT = 1


@task(requests=Resources(cpu="4", mem="4Gi"), limits=Resources(cpu="4", mem="4Gi"))
def generate(hdfs_output: str, scale_factor: int, num_parts: int, partition: int) -> str:
    start = time.time()

    execute("./flyte/workflows/dsdgen -dir . -force Y -scale %d -child %d -parallel %d" % (scale_factor, partition, num_parts))
    for t in glob.glob("*.dat"):
        copy_table_to_hdfs(hdfs_output=hdfs_output, table_name=re.sub(r"_\d+_\d+.dat", "", t), data_file=t)
        os.remove(t)

    return str(time.time() - start)


def copy_table_to_hdfs(hdfs_output: str, table_name: str, data_file: str):
    """Upload data to HDFS"""
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.load_system_host_keys()
    ssh.connect(hostname="192.168.103.135", port=22, username="charles", password="char0220")
    scp = SCPClient(ssh.get_transport())

    scp.put(data_file, remote_path="/home/charles/tmp")
    scp.close()
    sc = "/home/charles/hadoop-3.2.3/bin/hadoop fs -mkdir -p %s" % hdfs_output
    stdin, stdout, stderr = ssh.exec_command(sc)
    out, err = stdout.read(), stderr.read()
    sc = "/home/charles/hadoop-3.2.3/bin/hadoop fs -mkdir -p %s/%s" % (hdfs_output, table_name)
    stdin, stdout, stderr = ssh.exec_command(sc)
    out, err = stdout.read(), stderr.read()
    sc = "/home/charles/hadoop-3.2.3/bin/hadoop fs -copyFromLocal -f /home/charles/tmp/%s %s/%s/" % (data_file, hdfs_output, table_name)
    stdin, stdout, stderr = ssh.exec_command(sc)
    out, err = stdout.read(), stderr.read()
    sc = "rm /home/charles/tmp/%s" % data_file
    stdin, stdout, stderr = ssh.exec_command(sc)
    out, err = stdout.read(), stderr.read()
    ssh.close()


def execute(cmd, retries_remaining=10):
    """Execute command"""
    if retries_remaining < 0:
        print("All retries for {cmd} exhauseted. Failing the attempt")
        sys.exit(1)

    try:
        subprocess.check_call(cmd, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError:
        backoff_time = (11-retries_remaining)*random.randint((11-retries_remaining)*MIN_BACKOFF_UNIT, MAX_BACKOFF_UNIT)
        print("command {cmd} failed. Retries remaining {retries_remaining}. Sleeping for {backoff_time} before trying again")
        time.sleep(backoff_time)
        execute(cmd, retries_remaining-1)


@task
def createexternaltables(host: str, port: int, dbname: str) -> str:
    """Variable setup"""
    start = time.time()
    tablenames = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer_address",
                  "customer_demographics", "customer", "date_dim", "household_demographics", "income_band",
                  "inventory", "item", "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
                  "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
    cmds = [externaltables.call_center, externaltables.catalog_page, externaltables.catalog_returns,
            externaltables.catalog_sales, externaltables.customer_address, externaltables.customer_demographics,
            externaltables.customer, externaltables.date_dim, externaltables.household_demographics,
            externaltables.income_band, externaltables.inventory, externaltables.item, externaltables.promotion,
            externaltables.reason, externaltables.ship_mode, externaltables.store_returns, externaltables.store_sales,
            externaltables.store, externaltables.time_dim, externaltables.warehouse, externaltables.web_page,
            externaltables.web_returns, externaltables.web_sales, externaltables.web_site]

    conn = hive.Connection(host, port, "charles", "default")
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {dbname} CASCADE")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    cursor.execute(f"use {dbname}")
    for i in range(0, 24):
        cursor.execute(f"drop table if exists {tablenames[i]}")
        cmd = "create external table " + tablenames[i] + cmds[i]
        cursor.execute(cmd)
    cursor.close()
    return str(time.time()-start)


@task(requests=Resources(cpu="1", mem="2Gi"), limits=Resources(cpu="2", mem="4Gi"))
def query(host: str, port: int, dbname: str) -> str:
    start = time.time()
    conn = hive.connect(host, port, "charles", "default")
    cursor = conn.cursor()
    cursor.execute(f"use {dbname}")
    # ff = open('flyte/workflows/queries.log', 'w')

    for i in range(1, 100):
        print(f"--------------------------")
        print(f"Preparing to do query{i}.")
        print(f"--------------------------")
        with open(f'flyte/workflows/queries/query{i}.sql', 'r') as f:
            data = f.read().replace(';', '')
        cursor.execute(data)
        '''logg = cursor.fetchall()
        for line in logg:
            # ff.write(line + '\n')
            print(line)
    # ff.close()'''
    return str(time.time()-start)


@workflow
def loadTest(hdfs_output: str, scale_factor: int) -> str:
    """Generate data using dsdgen"""

    num_parts = 24
    loadtime = time.time()
    for partition in range(1, num_parts+1):
        '''execute("./flyte/workflows/dsdgen -dir . -force Y -scale %d -child %d -parallel %d" % (scale_factor, partition, num_parts))
        print(f"Completed : ./flyte/workflows/dsdgen -dir . -force Y -scale {scale_factor} -child {partition} -parallel {num_parts}")
        for t in glob.glob("*.dat"):
            copy_table_to_hdfs(hdfs_output=hdfs_output, table_name=re.sub(r"_\d+_\d+.dat", "", t), data_file=t)
            os.remove(t)'''
        generate(hdfs_output=hdfs_output, scale_factor=scale_factor, num_parts=num_parts, partition=partition)
    return str(time.time() - loadtime)


@workflow
def tpcds():
    """Variable Setup"""
    hdfs_dir = "/test/maptask24"
    scale = 10
    host = "192.168.103.135"
    port = 10000
    dbname = "maptask24"

    """Generate data"""
    load_promise = loadTest(hdfs_output=hdfs_dir, scale_factor=scale)

    """Create tables"""
    table_promise = createexternaltables(host=host, port=port, dbname=dbname)

    """Query"""
    query_promise = query(host=host, port=port, dbname=dbname)

    load_promise >> table_promise
    table_promise >> query_promise
    print(f"{load_promise}, {table_promise}, {query_promise}")


if __name__ == "__main__":
    print(f"Running tpcds() { tpcds() }")
