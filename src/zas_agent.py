import os
import sys
import ConfigParser
import argparse
import struct
import multiprocessing
import socket

ARGS=None
SCENARIO=None

def handle(connection, address, scenario, args):
    import logging

    ##
    ## Working with keys
    ##


    def locate_key(scenario, data):
        import fnmatch,re

        key = data.strip()
        if key in scenario.sections():
            return scenario.get(key, "value")
        else:
            print scenario.sections()
            for s in scenario.sections():
                try:
                    patt = scenario.get(s, "match")
                except:
                    print "NO",s
                    continue
                print "SCAN",s
                if patt == key or fnmatch.fnmatch(key, patt) or re.match(patt, key) != None:
                    return scenario.get(s, "value")
                print s,patt,key,re.match(patt, key)
        return None

    ##
    ## Generate data
    ##

    def generate_random_uniform(val):
        import numpy as np
        try:
            low,high = val.split(",")
            return np.random.uniform(float(low),float(high))
        except:
            return None
    def get_data_from_redis(host, port, val):
        import redis
        try:
            r = redis.Redis(host=host, port=port, db=0)
            res = r.get(val)
            del r
            return res
        except:
            return None
    def get_data_from_redis_queue(host, port, val):
        import redis
        try:
            r = redis.Redis(host=host, port=port, db=1)
            res = r.lindex(val, r.llen(val)-1)
            del r
            return res
        except KeyboardInterrupt:
            return None


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
            print "REDIS",r_key
            res =get_data_from_redis(args.redis_host, args.redis_port, r_key)
            if not res:
                return None
            return str(res)
        elif v_type == "rqueue":
            if len(val) == 0:
                r_key = data
            else:
                r_key = val
            print "REDIS",r_key
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
            data = connection.recv(5)
            try:
                hdr = struct.unpack("ssssB", data)
            except:
                break
            sig = "".join(list(hdr[:4]))
            ver = list(hdr)[-1]
            if sig != "ZBXD":
                logger.debug("Zabbix signature not found")
                break
            data = connection.recv(8)
            p_len = struct.unpack("L", data)[0]
            if p_len < 0 and p_len > 1024:
                logger.debug("Request is too small or too large")
                break
            data = connection.recv(p_len)
            if data == "":
                logger.debug("Socket closed remotely")
                break
            if ver == 1:
                data = data.strip()
                r_data = protocol_v1(scenario, args, data)
            if not r_data:
                logger.debug("Can not locate value for the key %s in scenario"%data)
                break
            hdr="ZBXD"+struct.pack("B",ver)+struct.pack("L",len(r_data)+1)
            connection.sendall(hdr+r_data+"\n")
            logger.debug("Sent data: %s=%s"%(repr(data),r_data))
    except:
        logger.exception("Problem handling request")
    finally:
        logger.debug("Closing socket")
        connection.shutdown(socket.SHUT_RDWR)
    sys.exit(0)

class Server(object):
    def __init__(self, hostname, port, scenario, _args):
        import logging
        self.logger = logging.getLogger("server")
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



def main():
    global ARGS
    args = ARGS

    import logging

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

def stop():
    global ARGS
    args = ARGS

    import time
    import signal

    if os.path.exists(args.pid):
        try:
            pid = int(open(args.pid).read())
        except:
            print "Can not read PID of the zas_agent"
            sys.exit(0)
        try:
            print "Terminating zas_agent:",
        except:
            print "can't"
        finally:
            print "ok"
        os.kill(pid, signal.SIGTERM)
        time.sleep(5)
        print "Killing zas_agent:",
        try:
            os.kill(pid, signal.SIGKILL)
        except:
            print "dead already"
    else:
        print "ZAS Agent isn't running..."

if __name__ == "__main__":
    from daemonize import Daemonize

    parser = argparse.ArgumentParser(description='Zabbix Agent Simulator')
    HOST, PORT = "0.0.0.0", 10050
    parser.add_argument('--listen', type=str, default=HOST, help='Listen IP')
    parser.add_argument('--port', type=int, default=PORT, help='Listen Port')
    parser.add_argument('--scenario', type=file, default=open("/etc/zas_scenario.cfg"), help="Path to scenario file")
    parser.add_argument('--redis_host', type=str, default="localhost", help='REDIS IP')
    parser.add_argument('--redis_port', type=int, default=6379, help='REDIS Port')
    parser.add_argument('--start', action='store_true', help="Start simulator")
    parser.add_argument('--stop', action='store_true', help="Stop simulator")
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
    if not ARGS.stop or ARGS.start:
        if os.path.exists(ARGS.pid):
            print "ZAS Agent is already running ..."
            sys.exit(0)
        daemon = Daemonize(app="zas_agent", pid=ARGS.pid, action=main, foreground=not ARGS.daemonize, user=ARGS.user, group=ARGS.group)
        daemon.start()
    elif ARGS.stop and not ARGS.start:
        stop()
    else:
        print "You have to specify --start or --stop"
        parser.print_help()

