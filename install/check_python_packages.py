#!/usr/bin/python

PKGS=['os','sys','ConfigParser','argparse','struct','multiprocessing','socket','time','logging','redis','numpy','fnmatch','re','signal','daemonize','simplejson']

for p in PKGS:
    import imp
    m_repr = "'%s'"%p
    m_repr = m_repr.ljust(20,".")
    print "Module %s"%m_repr,
    try:
        fp, pathname, description = imp.find_module(p)
        imp.load_module(p, fp, pathname, description)
    except:
        print "FAIL"
        continue
    finally:
        print "OK"
