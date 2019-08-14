#!/usr/bin/python                                                                            
                                                                                             
from mininet.topo import Topo
from mininet.net import Mininet
#from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.cli import CLI
import json
import os
import shutil 

from time import sleep

# Given a list of (host,port) pairs, generate a list of them as dictionaries
def nodes(hps):
    return_val = []
    for host, port in hps:
        return_val.append ( { "host" : host, "port" : port } )
    return return_val

# For given lists of (host,port) pairs, generate the dictionary that nominates each to a role
def configuration(clients, replicas, leaders, acceptors):
    return { "leaders" : nodes(leaders),
             "replicas" : nodes(replicas),
             "clients" : nodes(clients),
             "acceptors" : nodes(acceptors) }

# Create a configuration JSON file with specified filename and number of nodes
# Ports are assigned automatically as 7000 (for now)
# IP Adresses are generated beginning with 10.0.0.1 and incrementing for each node
# (This may have to be changed when simulated networks become more complicated)
def create_config(filename, client_n, replica_n, leader_n, acceptor_n):
    j = 1
    clients = []
    for i in range(0,client_n):
        clients.append (("10.0.0." + str(j), 7000))
        j += 1
    replicas = []
    for i in range(0,replica_n):
        replicas.append (("10.0.0." + str(j), 7000))
        j += 1
    acceptors = []
    for i in range(0,acceptor_n):
        acceptors.append (("10.0.0." + str(j), 7000))
        j += 1    
    leaders = []
    for i in range(0,leader_n):
        leaders.append (("10.0.0." + str(j), 7000)) 
        j +=1 
    config = configuration(clients, replicas, leaders, acceptors)
    json_str = json.dumps(config, indent=2)

    print json_str

    json_file = open(filename, "w")
    json_file.write(json_str)
    json_file.close()

# Generate a single-switch star topology with n nodes
# Network parameters (bw, loss, etc) are hard-coded for now
class SingleSwitchTopo(Topo):
    def build(self, n=2, lossy=False):
        switch = self.addSwitch('s1')
        for h in range(n):
	    # Each host gets 100%/n of system CPU
	    host = self.addHost( 'h%s' % (h + 1) )

           # 1000 Mbps, 1ms delay, 0% loss, 1000 packet queue
	    self.addLink( host, switch, bw=1000, delay='5ms', loss=0,
                          max_queue_size=1000, use_htb=True )

def run(host, role, ip, port, config_path):
    result = host.cmd('eval $(opam config env) && ocamlrun main.bc ' + 
             ' --node ' + role +
             ' --host ' + ip +
             ' --port ' + str(port) +
             ' --config ' + config_path + ' &')
    print result
    
def run_client(host, role, ip, port, config_path, n):
    result = host.cmd('eval $(opam config env) && ocamlrun main.bc ' + 
             ' --node ' + role +
             ' --host ' + ip +
             ' --port ' + str(port) +
             ' --config ' + config_path + 
             ' --trace ' + str(n) + ' &')
    print result

def runCrashSimulation(client_n, replica_n, leader_n, acceptor_n, sim_length1, sim_length2):
    print "Running test simulation"
        
    total_n = client_n + replica_n + leader_n + acceptor_n

    # Generate the configuration file
    config_path = './config-sim.json'
    create_config(config_path, client_n, replica_n, leader_n, acceptor_n) 

    # Generate topology for given system
    topo = SingleSwitchTopo(n=total_n)
    net = Mininet( topo, link=TCLink )
    
    # Start the simulation
    net.start()
    
    # Do some setup test stuff ...
    print "Dumping host connections"
    dumpNodeConnections(net.hosts)
 
    # Counter for _._._.x part of IP addresses of non-client nodes
    # We init clients last so leave their addresses free
    j = client_n + 1
    
    # Initialize replicas
    for i in range(1, replica_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up replica " + str(i) + " on " + h.IP() + ":" + "7000" )
        run(h, 'replica', h.IP(), 7000, config_path)
        j += 1

    sleep(1)    

    # Initialize acceptors
    for i in range(1, acceptor_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up accceptor " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run(h, 'acceptor', h.IP(), 7000, config_path)
        j += 1

    sleep(1)    

    # Initialize leaders
    for i in range(1, leader_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up leader " + str(i) + " on " + h.IP() + ":" + "7000" )                
        run(h, 'leader', h.IP(), 7000, config_path)
        sleep (0.5)
        j += 1

    sleep(1)

    # Get the pid of the node we need to kill before the simulation starts
    k = 3
    h_kill = net.get('h' + str(k))    
    uh = h_kill.cmd('echo')
    pid = h_kill.cmd('ps | awk \'/[o]camlrun/{print $1}\'')
    print pid

    # Initialize clients
    # (Do clients last as we need all nodes to be initialized before requests start firing)
    for i in range(1, client_n + 1):
        h = net.get('h' + str(i))
        print ( "Spin up client " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run_client(h, 'client', h.IP(), 7000, config_path, 1000)    
    
    # Run the simulation for sim_length seconds then stop it
    sleep(sim_length1)

    # Kill the node
    result = h_kill.cmd('kill -9 ' + str(pid))
    print result

    sleep(sim_length2)

    # Kill the simulation
    net.stop();
    
    # Delete the auto-generated configuration file
    os.remove(config_path)


def runCrashRestoreSimulation(client_n, replica_n, leader_n, acceptor_n, sim_length1, sim_length2, sim_length3):
    print "Running test simulation"
        
    total_n = client_n + replica_n + leader_n + acceptor_n

    # Generate the configuration file
    config_path = './config-sim.json'
    create_config(config_path, client_n, replica_n, leader_n, acceptor_n) 

    # Generate topology for given system
    topo = SingleSwitchTopo(n=total_n)
    net = Mininet( topo, link=TCLink )
    
    # Start the simulation
    net.start()
    
    # Do some setup test stuff ...
    print "Dumping host connections"
    dumpNodeConnections(net.hosts)
 
    # Counter for _._._.x part of IP addresses of non-client nodes
    # We init clients last so leave their addresses free
    j = client_n + 1
    
    # Initialize replicas
    for i in range(1, replica_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up replica " + str(i) + " on " + h.IP() + ":" + "7000" )
        run(h, 'replica', h.IP(), 7000, config_path)
        j += 1

    sleep(1)    

    # Initialize acceptors
    for i in range(1, acceptor_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up accceptor " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run(h, 'acceptor', h.IP(), 7000, config_path)
        j += 1

    sleep(1)    

    # Initialize leaders
    for i in range(1, leader_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up leader " + str(i) + " on " + h.IP() + ":" + "7000" )                
        run(h, 'leader', h.IP(), 7000, config_path)
        sleep (0.5)
        j += 1

    sleep(1)

    # Get the pid of the node we need to kill before the simulation starts
    k = 3
    h_kill = net.get('h' + str(k))    
    uh = h_kill.cmd('echo')
    pid = h_kill.cmd('ps | awk \'/[o]camlrun/{print $1}\'')
    print pid

    # Initialize clients
    # (Do clients last as we need all nodes to be initialized before requests start firing)
    for i in range(1, client_n + 1):
        h = net.get('h' + str(i))
        print ( "Spin up client " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run_client(h, 'client', h.IP(), 7000, config_path, 1000)    
    
    # Run the simulation for sim_length seconds then stop it
    sleep(sim_length1)

    # Kill the node
    result = h_kill.cmd('kill -9 ' + str(pid))
    print result

    sleep(sim_length2)

    # Restore the node
    run(h_kill, 'replica', h_kill.IP(), 7000, config_path)

    sleep(sim_length3)

    # Kill the simulation
    net.stop();
    
    # Delete the auto-generated configuration file
    os.remove(config_path)

def runSimulation(client_n, replica_n, leader_n, acceptor_n, sim_length):
    print "Running test simulation"
        
    total_n = client_n + replica_n + leader_n + acceptor_n

    # Generate the configuration file
    config_path = './config-sim.json'
    create_config(config_path, client_n, replica_n, leader_n, acceptor_n) 

    # Generate topology for given system
    topo = SingleSwitchTopo(n=total_n)
    net = Mininet( topo, link=TCLink )
    
    # Start the simulation
    net.start()
    
    # Do some setup test stuff ...
    print "Dumping host connections"
    dumpNodeConnections(net.hosts)
 
    # Counter for _._._.x part of IP addresses of non-client nodes
    # We init clients last so leave their addresses free
    j = client_n + 1
    
    # Initialize replicas
    for i in range(1, replica_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up replica " + str(i) + " on " + h.IP() + ":" + "7000" )
        run(h, 'replica', h.IP(), 7000, config_path)
        j += 1
    sleep(1)    

    # Initialize acceptors
    for i in range(1, acceptor_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up accceptor " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run(h, 'acceptor', h.IP(), 7000, config_path)
        j += 1
    sleep(1)    

    # Initialize leaders
    for i in range(1, leader_n + 1):
        h = net.get('h' + str(j))
        print ( "Spin up leader " + str(i) + " on " + h.IP() + ":" + "7000" )                
        run(h, 'leader', h.IP(), 7000, config_path)
        sleep (0.5)
        j += 1
    sleep(1)

    # Initialize clients
    # (Do clients last as we need all nodes to be initialized before requests start firing)
    for i in range(1, client_n + 1):
        h = net.get('h' + str(i))
        print ( "Spin up client " + str(i) + " on " + h.IP() + ":" + "7000" )        
        run_client(h, 'client', h.IP(), 7000, config_path, 100)    
    
    # Run the simulation for sim_length seconds then stop it
    sleep(sim_length)

    # Kill the simulation
    net.stop();
    
    # Delete the auto-generated configuration file
    os.remove(config_path)


if __name__ == '__main__':
    setLogLevel('info')

    #for k in range(7,10):
    
    # 5ms delay on links
    #runCrashSimulation(1,3,1,3,30,30)
    #shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/acceptor-crash-base.log")
    
    
    #for k in range(11,20):
    #    runSimulation(1,1,1,k,30)
    #    shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/commit-latency/trace-" + str(k) + ".log")

    #for k in range(1,21):
    #    runSimulation(1,1,1,k,30)
    #    shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/commit-latency/acceptor-trace-" + str(k) + ".log")
    
    #for k in range(3,21):
    #    runCrashSimulation(1,3,1,3,30,30)
    #    shutil.move("logs/client-10.0.0.1-7000-info.log", "share/bandwidth-test-" + str(k) + ".log")

    # runCrashSimulation(1,3,1,3,30,90)
    #shutil.move("logs/client-10.0.0.1-7000-info.log", "share/bandwidth-testo-1.log")
    #shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/latency-testo-1.log")
    
    for k in range(2,3):
        runSimulation(1,k+1,k+1,2*k +1,60)
        shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/commit-latency/f-trace-" + str(k) + ".log")

    # (5ms delay on links)
    #for k in range(5,11):
    #    runCrashRestoreSimulation(1,3,1,3,30,15,15)
    #    shutil.move("logs/client-10.0.0.1-7000-trace.log", "replica-crash-restore-" + str(k) + ".log")
    
    

    #replica_n = 10
    #for k in range(1,4):
    #    os.makedirs("share/logs/run-0-" + str(k))
    #    for i in range(1,replica_n + 1):
    #        runSimulation(1,2,i,3,max(60,10*i))
    #        f = open("logs/client-10.0.0.1-7000-trace.log", "r")
    #        print f.read()
    #        shutil.move("logs/client-10.0.0.1-7000-trace.log", "share/logs/run-0-" + str(k) + "/trace-" + str(i) + ".log")
