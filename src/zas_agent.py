#!/usr/bin/python
##
##
## Zabbix Passive Agent Simulator: version 0.1
##
##

import os
import sys
import ConfigParser
import argparse
import struct
import multiprocessing
import socket
import time
import logging

ARGS=None
SCENARIO=None

##
## def handle(connection, address, scenario, args)
## agent request handler
##  - connection: Incoming connection handler
##  - address: Incoming address
##  - scenario: Initial simulation scenario
##  - args: Passed parameters which were passed to the main program
##


def handle(connection, address, scenario, args):
    import logging

    ##
    ## Working with keys:
    ## locate_key(scenario, data)
    ## locate key requested by zabbix_get in the scenario
    ##  - scenario: current scenario
    ##  - data: data passed from zabbix_get
    ##
    def locate_key(scenario, data):
        import fnmatch,re

        key = data.strip()
        if key in scenario.sections():
            return scenario.get(key, "value")
        else:
            for s in scenario.sections():
                try:
                    patt = scenario.get(s, "match")
                except:
                    continue
                if patt == key or fnmatch.fnmatch(key, patt) or re.match(patt, key) != None:
                    return scenario.get(s, "value")
        return None

    ##
    ## Generate data
    ##

    ##
    ## Generating random data within the range val=low,high
    ##
    def generate_random_uniform(val):
        import numpy as np
        try:
            low,high = val.split(",")
            return np.random.uniform(float(low),float(high))
        except:
            return None
    ##
    ## Request data from REDIS store
    ##  - host: Redis IP
    ##  - port: Redis port
    ##  - val: data passed from zabbix_agent
    ##
    def get_data_from_redis(host, port, val):
        import redis
        try:
            r = redis.Redis(host=host, port=port, db=0)
            res = r.get(val)
            del r
            return res
        except:
            return None
    ##
    ## Request data from REDIS store which were stored in the list
    ## This function requests the last data in the list
    ## You must apped data to the list with LPUSH
    ##  - host: Redis IP
    ##  - port: Redis port
    ##  - val: data passed from zabbix_agent
    ##
    def get_data_from_redis_queue(host, port, val):
        import redis
        try:
            r = redis.Redis(host=host, port=port, db=1)
            res = r.lindex(val, r.llen(val)-1)
            del r
            return res
        except KeyboardInterrupt:
            return None

    ##
    ## Handler for the Zabbix protocol V 1
    ##
    def protocol_v1(scenario, args, data):
        value = locate_key(scenario, data)
        if not value:
            return None
        ix = value.index(":")
        v_type, val = value[:ix],value[ix+1:].strip()
        v_type = v_type.lower()
        if v_type == "static":
            return str(val)
        elif v_type == "uniform_int":
            res = generate_random_uniform(val)
            if not res:
                return res
            else:
                return str(int(res))
        elif v_type == "uniform":
            try:
                return str(generate_random_uniform(val))
            except:
                return None
        elif v_type == "redis":
            if len(val) == 0:
                r_key = data
            else:
                r_key = val
            res =get_data_from_redis(args.redis_host, args.redis_port, r_key)
            if not res:
                return None
            return str(res)
        elif v_type == "rqueue":
            if len(val) == 0:
                r_key = data
            else:
                r_key = val
            res =get_data_from_redis_queue(args.redis_host, args.redis_port, r_key)
            if not res:
                return None
            return str(res)

        else:
            return None

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("process-%r" % (address,))
    try:
        logger.debug("Connected %r at %r", connection, address)
        while True:
            try:
                data = connection.recv(1024)
            except:
                break
            try:
                hdr = struct.unpack("ssssB", data[:5])
            except:
                break
            sig = "".join(list(hdr[:4]))
            ver = list(hdr)[-1]
            if sig != "ZBXD":
                ver=10
            else:
                p_len = struct.unpack("L", data[5:13])[0]
                if p_len < 0 and p_len > 1024:
                    logger.debug("Request is too small or too large")
                    break
                data = data[13:]
            if data == "":
                logger.debug("Socket closed remotely")
                break
            if ver == 1:
                data = data.strip()
                r_data = protocol_v1(scenario, args, data)
            else:
                data = data.strip()
                r_data = protocol_v1(scenario, args, data)
            if not r_data:
                logger.debug("Can not locate value for the key %s in scenario"%data)
                break
            if ver in [1,10]:
                hdr="ZBXD"+struct.pack("B",1)+struct.pack("L",len(r_data)+1)
                connection.sendall(hdr+r_data+"\n")
            logger.debug("Sent data: %s=%s"%(repr(data),r_data))
    except:
        logger.exception("Problem handling request")
    finally:
        logger.debug("Closing socket")
        try:
            connection.close()
        except:
            pass
    sys.exit(0)

##
## Implementation of the TCP multiprocessing Server
##
class Server(object):
    def __init__(self, hostname, port, scenario, _args):
        import logging
        self.logger = logging.getLogger("zas_server")
        self.hostname = hostname
        self.port = port
        self.scenario = scenario
        self.args = _args

    def start(self):
        global SCENARIO
        self.logger.debug("listening")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.hostname, self.port))
        self.socket.listen(1)

        while True:
            conn, address = self.socket.accept()
            self.logger.debug("Got connection")
            process = multiprocessing.Process(target=handle, args=(conn, address, self.scenario, self.args))
            process.daemon = True
            process.start()
            self.logger.debug("Started process %r", process)


##
## Feed the REDIS with pseudo-random stuff
##
def gen_ingest():
    global ARGS
    args = ARGS

    import redis

    ##
    ## Calculate percentage
    ##
    def percentage(part, whole):
        return 100 * float(part)/float(whole)
    ##
    ## Let's read ingestion scenario
    ##
    def parse_ingestion_scenario(fname):
        import simplejson

        logger = logging.getLogger("zas_ingestor")
        if not os.path.exists(fname):
            logger.error("Ingestion scenario file %s not exists"%fname)
            return {}
        f = open(fname)
        scn = {}
        lines = f.readlines()
        for l in lines:
            if l[0] in ["#", ";"]:
                ## This is comment
                continue
            ix = l.index(":")
            s_key,s_scn = l[:ix],l[ix+1:].strip()
            try:
                scn[s_key] = simplejson.loads(s_scn)
            except:
                continue
        return scn
    ##
    ## Let's ingest single scenario
    ##
    def ingest_scenario(r, key, scn):
        import numpy as np

        def push_spike_to_redis(r, key, array):
            k = "%s.spike"%key
            for v in array:
                r.rpush(k,v)
        def return_res(scn, val):
            if scn["type"] == "int":
                return int(val)
            return val

        s_min = float(scn["min"])
        s_max = float(scn["max"])
        if r.llen("%s.spike"%key) > 0:
            s_val = float(r.lpop("%s.spike"%key))
            r.set(key,s_val)
            return return_res(scn,s_val)
        if scn.has_key("key"):
            req_key = scn["key"]
        else:
            req_key = key
        prev_val = r.get(req_key)
        if prev_val == None:
            prev_val = np.random.uniform(s_min,s_max)
        else:
            prev_val = float(prev_val)
        ##
        ## Throw the dice if there is a time for a spike
        ##
        if scn.has_key("spike_barrier") and scn.has_key("spike_width") and r.llen("%s.spike"%key) == 0:
            s_barrier = float(scn["spike_barrier"])
            dice = np.random.uniform(0,100)
            if s_barrier > dice:
                ## Spike !
                spike_max = np.random.uniform(prev_val,s_max)
                spike_min = np.random.uniform(spike_max, s_min)
                h_width = int(scn["spike_width"])/2
                h_plateu = int(np.random.uniform(1,h_width))
                up = np.linspace(prev_val, spike_max, h_width)
                plateu = np.random.gamma(spike_max, 0.95,h_plateu)
                down = np.linspace(spike_max, spike_min, h_width)
                push_spike_to_redis(r,key,up)
                push_spike_to_redis(r,key,plateu)
                push_spike_to_redis(r,key,down)
                print "Spike!",dice,s_barrier,up,plateu,down


        if scn.has_key("variation_min") and scn.has_key("variation_max"):
            min_val = prev_val-(prev_val*0.01)*float(scn["variation_min"])
            if min_val < s_min:
                min_val = s_min
            max_val = prev_val+(prev_val*0.01)*float(scn["variation_max"])
            if max_val > s_max:
                max_val = s_max
            cur_val = np.random.uniform(min_val, max_val)
        elif scn.has_key("variation_rnd") and scn["variation_rnd"] == 1:
            min_val = np.random.uniform(float(scn["min"]),float(prev_val))
            max_val = np.random.uniform(float(prev_val),float(scn["max"]))
            cur_val = np.random.uniform(min_val, max_val)
        else:
            cur_val = np.random.uniform(float(scn["min"]),float(scn["max"]))
        cur_val = return_res(scn,cur_val)
        r.set(key,cur_val)
    logger = logging.getLogger("zas_ingestor")
    while True:
        r = redis.Redis(host=ARGS.redis_host, port=ARGS.redis_port, db=0)
        scn = parse_ingestion_scenario(ARGS.ingest_scenario)
        logger.debug("%d keys are found in ingestion scenario"%len(scn.keys()))
        for k in scn.keys():
            ingest_scenario(r,k,scn[k])
        del r
        time.sleep(ARGS.ttl)

##
## Fill the REDIS with stuff from files
##
def _ingest(ARGS, fun, use_ttl=False):
    if ARGS.ingest_file != "-" and os.path.exists(ARGS.ingest_file):
        f = open(ARGS.ingest_file)
    elif ARGS.ingest_file == "-":
        f = sys.stdin
    else:
        print "Ingest data file %s not found"%ARGS.ingest_file
        sys.exit()
    while True:
        line = f.readline().strip()
        if len(line) == 0:
            f.close()
            break
        if line[0] in ["#",";"]:
            continue
        ix = line.index(":")
        ig_key,ig_val = line[:ix],line[ix+1:]
        print ig_key, ig_val
        fun(ig_key, ig_val)
        if use_ttl:
            time.sleep(int(ARGS.ttl))
##
## Ingest to redis:
##
def ingest(ARGS):
    import redis

    r = redis.Redis(host=ARGS.redis_host, port=ARGS.redis_port, db=0)
    _ingest(ARGS, r.set)
    del r
    sys.exit(0)
##
## Ingest to rqueue:
##
def rq_ingest(ARGS):
    import redis

    r = redis.Redis(host=ARGS.redis_host, port=ARGS.redis_port, db=1)
    _ingest(ARGS, r.lpush, True)
    del r
    sys.exit(0)

##
## Clean-up rqueue:
##
def rq_cleanup(ARGS):
    import redis

    while True:
        time.sleep(int(ARGS.ttl))
        r = redis.Redis(host=ARGS.redis_host, port=ARGS.redis_port, db=1)
        for key in r.keys():
            if (r.llen(key) == 1 and ARGS.rq_cleanup_full) or r.llen(key) > 1:
                r.rpop(key)
                time.sleep(int(ARGS.ttl))
        del r

##
## Function MAIN()
##
def main():
    global ARGS
    args = ARGS

    try:
        SCENARIO = ConfigParser.SafeConfigParser()
        SCENARIO.readfp(args.scenario)
    except KeyboardInterrupt:
        print "Can not parse scenario file"
        sys.exit(0)

    if args.daemonize:
        logging.basicConfig(filename=args.log,level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.DEBUG)

    server = Server(args.listen, args.port, SCENARIO, args)
    try:
        logging.info("Listening %s:%d"%(args.listen, args.port))
        server.start()
    except:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Shutting down")
        for process in multiprocessing.active_children():
            logging.info("Shutting down process %r", process)
            process.terminate()
            process.join()
    logging.info("All done")
##
## Stop the daemon
##
def stop():
    global ARGS
    args = ARGS

    import time
    import signal

    if os.path.exists(args.pid):
        try:
            pid = int(open(args.pid).read())
        except:
            print "Can not read PID of the ZAS"
            sys.exit(0)
        try:
            print "Terminating ZAS:",
        except:
            print "can't"
        finally:
            print "ok"
        os.kill(pid, signal.SIGTERM)
        time.sleep(5)
        print "Killing ZAS:",
        try:
            os.kill(pid, signal.SIGKILL)
        except:
            print "dead already"
    else:
        print "ZAS isn't running..."

if __name__ == "__main__":
    from daemonize import Daemonize

    parser = argparse.ArgumentParser(description='Zabbix Agent Simulator')
    HOST, PORT = "0.0.0.0", 10050
    parser.add_argument('--listen', type=str, default=HOST, help='Listen IP')
    parser.add_argument('--port', type=int, default=PORT, help='Listen Port')
    parser.add_argument('--scenario', type=file, default=open("/etc/zas_scenario.cfg"), help="Path to scenario configuration file")
    parser.add_argument('--redis_host', type=str, default="localhost", help='REDIS IP')
    parser.add_argument('--redis_port', type=int, default=6379, help='REDIS Port')
    parser.add_argument('--start', action='store_true', help="Start simulator")
    parser.add_argument('--stop', action='store_true', help="Stop simulator")
    parser.add_argument('--ingest', action='store_true', help="Ingest data into REDIS for redis: metrics")
    parser.add_argument('--rq_ingest', action='store_true', help='Ingest data into REDIS for rqueue: metrics')
    parser.add_argument('--ingest_file', type=str, default="-", help='Path to the data file')
    parser.add_argument('--rq_cleanup', action='store_true', help='Cleanup data from the rqueue: metrics')
    parser.add_argument('--gen_ingest', action='store_true', help='Read \"ingest scenario\" and feed redis: metrics')
    parser.add_argument('--ingest_scenario', type=str, default="-", help='Path to ingest scenario configuration file')
    parser.add_argument('--ttl', type=int, default=15, help='TTL (Time To Live) for the metrics (in seconds)')
    parser.add_argument('--rq_cleanup_full', action='store_true', help="Clean up queues to empty state")
    parser.add_argument("--log", type=str, default="/tmp/zas_agent.log", help="Name of the log file if agent is daemonized")
    parser.add_argument('--daemonize', action='store_true', help="Daemonize simulator")
    parser.add_argument('--pid', type=str, default="/tmp/zas_agent.pid", help="PID file for simulator process")
    parser.add_argument('--user', type=str, default="zabbix", help='Run simulator as this user')
    parser.add_argument('--group', type=str, default="zabbix", help='Run simulator with this GUID')


    try:
        ARGS = parser.parse_args()
    except Exception, msg:
        #traceback.print_exception()
        print "Error occurs:", msg
        parser.print_help()
        sys.exit(0)
    if not ARGS.stop and ARGS.start and not ARGS.gen_ingest:
        if os.path.exists(ARGS.pid):
            print "ZAS Agent is already running ..."
            sys.exit(0)
        daemon = Daemonize(app="zas_agent", pid=ARGS.pid, action=main, foreground=not ARGS.daemonize, user=ARGS.user, group=ARGS.group)
        daemon.start()
    elif ARGS.stop and not ARGS.start:
        stop()
    elif ARGS.ingest:
        ingest(ARGS)
    elif ARGS.rq_ingest:
        rq_ingest(ARGS)
    elif ARGS.rq_cleanup:
        rq_cleanup(ARGS)
    elif ARGS.gen_ingest and ARGS.start:
        if os.path.exists(ARGS.pid):
            print "ZAS Ingestor is already running ..."
            sys.exit(0)
        if ARGS.ingest_scenario == "-":
            print "ZAS Ingestor can not use STDIN for scenario file"
            sys.exit(0)
        daemon = Daemonize(app="zas_ingestor", pid=ARGS.pid, action=gen_ingest, foreground=not ARGS.daemonize, user=ARGS.user, group=ARGS.group)
        daemon.start()
    else:
        print "You have to specify --start or --stop"
        parser.print_help()

