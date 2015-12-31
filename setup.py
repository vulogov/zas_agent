##
## Installtion of the ZAS Agent
##

from setuptools import setup

setup(
    name="zas_agent",
    author="Vladimir Ulogov",
    author_email="vladimir.ulogov@zabbix.com",
    license="GNU GPLv3",
    description="zas_agent",
    long_description="Zabbix Agent Simulator",
    version="0.1.1",
    scripts=['src/zas_agent.py',],
    data_files=[
        ("/etc/zas", ['etc/network.scenario'],),
        ("/etc", ["etc/zas_scenario.cfg"]),
        ("/usr/share/zas_agent", ["doc/zas-agent.0.1.1.pdf",]),
        ("/usr/share/man/man1", ["doc/zas_agent.py.1",])
    ],
    install_requires=[
        "daemonize >= 2.4.2",
        "numpy >= 1.4.1",
        "redis >= 2.0.0",
        "simplejson >= 2.0.9"
    ]
    )