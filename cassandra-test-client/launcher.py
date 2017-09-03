#!/usr/bin/python

'''Launches cassandra-stress instances in Marathon.'''

import json
import logging
import pprint
import random
import string
import sys
import os
import urllib
import MySQLdb
import pandas as pd
import base64
import datetime
mysql_cn = MySQLdb.connect(host='127.0.0.1',
                           port=3306, user='root', passwd='',
                           db='mds')

from configparser import ConfigParser
# non-stdlib libs:

class MdsConfig(object):
    """ Class that deals with all the external configuration """
    def __init__(self):
        """ Initialize with a home directory path.."""
        self.home = os.path.expanduser(os.getenv('MDS_HOME', '~/.mds'))

        self.config_file = self.home + '/config.ini'


        self.config = ConfigParser()
        self.config.read(self.config_file)

    def raw_get_config(self, section, option):
        """ Get config option value from config file """
        if self.config.has_option(section, option):
            return self.config.get(section, option)
        return None

    def get_secret(self, name):
        return self.raw_get_config('secrets', name)

    def get_global(self, name, default=None, config_type=None):
        """ Get a global config option, returning default if not specified, casting to whatever type is specified """
        if name is None:
            raise ValueError("Name can not be None")
        if not self.config.has_option('default', name):
            return default
        if config_type is None or config_type is str:
            return self.config.get('default', name, default=default)
        elif config_type is int:
            return self.config.getint('default', name, default=default)
        elif config_type is bool:
            return self.config.getboolean('default', name, default=default)
        elif config_type is float:
            return self.config.getfloat('default', name, default=default)

try:
    import click
    import requests
    from requests.exceptions import HTTPError
except ImportError:
    print("Failed to load third-party libraries.")
    print("Please run: $ pip install -r requirements.txt")
    sys.exit(1)

def __urljoin(*elements):
    return "/".join(elem.strip("/") for elem in elements)

def __post(url, headers={}, json=None):
    pprint.pprint(json)
    response = requests.post(url, json=json, headers=headers)
    return __handle_response("POST", url, response)

def __handle_response(httpcmd, url, response):
    # http code 200-299 => success!
    if response.status_code < 200 or response.status_code >= 300:
        errmsg = "Error code in response to %s %s: %s/%s" % (
            httpcmd, url, response.status_code, response.content)
        print(errmsg)
        raise HTTPError(errmsg)
    json = response.json()
    print("Got response for %s %s:\n%s" % (httpcmd, url, json))
    return json

def marathon_apps_url(cluster_uri):
    url = __urljoin(cluster_uri, "marathon", "v2", "apps")
    print("Marathon query: %s" % url)
    return url

def marathon_launch_app(marathon_url, app_id, cmd, instances=1, packages=[], env={}, headers={}):
    formatted_packages = []
    for package in packages:
        formatted_packages.append({"uri": package})
    formatted_env = {}
    for k,v in env.items():
        formatted_env[str(k)] = str(v)
    post_json = {
        "id": app_id,
        "container": {
            "type": "MESOS",
        },
        "cmd": cmd,
        "cpus": 1,
        "mem": 512.0, # 512m apparently required: 128m and 256m results in FAILEDs.
        "disk": 1,
        "instances": instances,
        "fetch": formatted_packages,
        "env": formatted_env,
    }

    json = __post(marathon_url, headers=headers, json=post_json)
    return json["deployments"]

def get_random_id(length=8):
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(length)])



def get_airlock_auth_token():
    """ Get Auth token from Airlock, None on error """
    mds_config = MdsConfig()
    url = mds_config.get_global("airlock_url")
    gh_token = mds_config.get_secret("github_token")
    if gh_token is None:
        raise ValueError("Config for {} not found.\nPlease setup CLI with `mds configure`".format("github_token"))
    try:
        headers = dict(Authorization="token " + gh_token)
        resp = requests.get(url=url, headers=headers)
    except Exception as exc:
        return None
    body = resp.json()
    if 'token' not in body:
        return None
    return body.get("token")


JRE_JAVA_PATH = "jre/bin/java"
CASSANDRA_STRESS_PATH = "apache-cassandra-*/tools/bin/cassandra-stress"
DEFAULT_PORT=9042


def get_dcos_name():
    curdate = str(datetime.date.today())
    curdate = curdate.replace("-", "")
    dcosname = "prasgarg" + curdate
    return dcosname

def get_dcos_url(dcosname):
    url = "http://emlb-%s-ue1.dev.mc-mds.corp.adobe.com/"%dcosname
    return url
def get_dcos_auth_token(frameworkname):
    sql = 'select properties from Cluster c join ClusterProperties p on c.id = p.id where c.clusterName = "%s"'%frameworkname
    df_mysql = pd.read_sql(sql, con=mysql_cn)
    dcosname = json.loads(df_mysql.iloc[0]["properties"])["dcosClusterName"]["value"];
    print dcosname
    sql = 'select userInfo from DcosCluster where name = "%s"' % dcosname
    df_mysql = pd.read_sql(sql, con=mysql_cn)
    userinfo = json.loads(df_mysql.iloc[0]["userInfo"])["users"][0]
    headers = {"Authorization": "Basic "  + base64.b64encode(userinfo["userId"] + ":"+ userinfo["password"])}
    tok_response = __post(__urljoin(get_dcos_url(dcosname), "acs/api/v1/auth/locallogin"),headers=headers)
    print tok_response
    return tok_response["token"]

@click.command()
# @click.argument('cluster_url', envvar='DCOS_URI')
@click.option("--framework-name", show_default=True, default='mdscee2b1',
              help="framework's name in DCOS, for auto-detecting nodes")
@click.option("--writer-count", show_default=True, default=1,
              help="number of writers to launch")
@click.option("--reader-count", show_default=True, default=0,
              help="number of readers to launch")
@click.option("--thread-count", show_default=True, default=5,
              help="number of threads to launch in each writer and reader")
@click.option("--duration", show_default=True, default='1h',
              help="amount of time for readers and writers to run before exiting")
@click.option("--consistency", show_default=True, default='one',
              help="consistency level to request for writers")
@click.option("--truncate", show_default=True, default='always',
              help="whether to truncate writes")
@click.option("--pkg-url", show_default=True, default="https://s3-us-west-1.amazonaws.com/prasgargtest/dcos-frameworks/mds-cassandra/dev/apache-cassandra-dev-3.0.10-bin-dcos.tar.gz",
              help="url of the cassandra package")
@click.option("--jre-url", show_default=True, default="/opt/mesosphere/active/java/usr/java",
              help="url of the jre package")
@click.option("--keyspace-override", default="",
              help="keyspace to use instead of a randomized default")
@click.option("--ip-override", default=[],
              help="list of node ips to use instead of what the framework returns")
@click.option("--port-override", show_default=True, default=DEFAULT_PORT,
              help="node port to use. cassandra-stress lacks support for multiple ports.")
@click.option("--username", envvar="DCOS_USERNAME",
              help="username to use when making requests to the DCOS cluster (if the cluster requires auth)")
@click.option("--password", envvar="DCOS_PASSWORD",
              help="password to use when making requests to the DCOS cluster (if the cluster requires auth)")

def main(
        framework_name,
        writer_count,
        reader_count,
        thread_count,
        duration,
        consistency,
        truncate,
        pkg_url,
        jre_url,
        keyspace_override,
        ip_override,
        port_override,
        username,
        password):
    """Launches zero or more test writer and reader clients against a Cassandra framework.

    The clients are launched as marathon tasks, which may be destroyed using the provided curl commands when testing is complete.

    You must at least provide the URL of the cluster, for example: 'python launcher.py http://your-dcos-cluster.com'"""
    cluster_url = get_dcos_url(get_dcos_name())
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger('requests.packages.urllib3')
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    keyspace_rand_id = get_random_id() # reused for keyspace, unless --keyspace-override is specified
    writer_app_id = 'cassandratest-' + keyspace_rand_id + '-writer'
    reader_app_id = 'cassandratest-' + keyspace_rand_id + '-reader'




    tok_response = get_dcos_auth_token(framework_name)
    print tok_response
    headers = {"Authorization": "token={}".format(tok_response)}

    if not ip_override:
        # args didn't specify manual ips, so fetch the list with a framework RPC:
        fetch_ips_path = '{}/service/{}/v1/nodes/connect/address'.format(cluster_url.rstrip("/"), framework_name)
        json = __handle_response('GET', fetch_ips_path, requests.get(fetch_ips_path, headers=headers))
        # strip ":port" from returned endpoints. the ":" confuses cassandra-stress, it parses them as IPv6 IPs.
        ips = []
        for endpoint in json:
            ip, port = endpoint.split(':')
            # if port arg is default, try using a port value returned by the framework
            if not port_override == DEFAULT_PORT:
                port_override = port
            ips.append(ip)
        ip_override = ','.join(ips)
        print('Using node IPs: {}, Port: {}'.format(ip_override, port_override))

    if not keyspace_override:
        # user didn't manually specify keyspace, generate random one (matches marathon job names)
        # 'can only contain alphanumeric and underscore characters'
        keyspace_override = 'test_' + keyspace_rand_id

    # Note that non-dashed args (eg "duration"/"truncate") must go first, followed by dashed args (eg "-node"/"-rate"):
    # See the available args here: https://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html

    common_args = 'duration={} cl={} truncate={} -node {} -mode native cql3 port={} -schema keyspace={} -rate threads={} user={} password={}'.format(
        duration, consistency, truncate, ip_override, port_override, keyspace_override, thread_count, username, password)
    reader_args = "counter_read {}".format(common_args)
    writer_args = "counter_write {}".format(common_args)

    marathon_url = marathon_apps_url(cluster_url)
    if not marathon_launch_app(
            marathon_url = marathon_url,
            app_id = reader_app_id,
            cmd = "export JAVA_HOME=%s && env && ${MESOS_SANDBOX}/%s %s" % (jre_url,
                CASSANDRA_STRESS_PATH, reader_args),
            instances = reader_count,
            packages = [pkg_url],
            headers = headers):
        print("Starting readers failed, skipping launch of writers")
        return 1
    if not marathon_launch_app(
            marathon_url = marathon_url,
            app_id = writer_app_id,
            cmd = "export JAVA_HOME=%s/jre && env && ${MESOS_SANDBOX}/%s %s" % (jre_url,
                CASSANDRA_STRESS_PATH, writer_args),
            instances = writer_count,
            packages = [pkg_url],
            headers = headers):
        print("Starting writers failed")
        return 1

    curl_headers = ""
    for k,v in headers.items():
        curl_headers += ' -H "{}: {}"'.format(k,v)
    print('''#################
Readers/writers have been launched.
When finished, delete them from Marathon with these commands:

curl -X DELETE{} {}/{}
curl -X DELETE{} {}/{}'''.format(
    curl_headers, marathon_url, reader_app_id,
    curl_headers, marathon_url, writer_app_id))
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

