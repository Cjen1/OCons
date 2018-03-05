import os
import numpy as np
import matplotlib.pyplot as plt
import sys
import math
import matplotlib.pyplot as plt
import scipy.stats as st

font = {'family' : 'normal',
        'weight' : 'bold',
        'size'   : 10}

plt.rc('font', **font)

def latency_from_str(str):
    latency = ''
    if line.endswith("ms\n"):
        latency = float ( line[0:len(line)-3] )
    elif line.endswith("m\n"):
        latency = float ( line[0:len(line)-2] ) * 60 * 1e4
    elif line.endswith("s\n"):
        latency = float ( line[0:len(line)-2] ) * 1e3
    return latency

def plot_latencies(filename,stepcolor='#555555',showsteps=True,showavg=True,avgcolor="#111111"):
    file = open(os.path.expanduser("~/share-test/" + filename), "r")
    latencies = []
    avgs = []
    for line in file.readlines ():
        latency = float( line[0:(len(line)-1)] )
        latencies.append(latency)
        if len(avgs) == 0:
            avgs = [latency]
        else:
            avgs.append( 0.875 * avgs[-1] + 0.125*latency )
    if showsteps:
        plt.plot(latencies, 
                 drawstyle='steps', 
                 color=stepcolor,
                 linewidth=0.7)
    if showavg:
        plt.plot(avgs,
                 color=avgcolor,
                 linewidth=0.7)
    return (latencies,avgs)

def avg_datasets(datasets):
    min_length = datasets[0]
    for k in range(0,len(datasets)):
        if len(datasets[k]) <= min_length:
            min_length = len(datasets[k])

    out = []
    for i in range(0, min_length):
        data = []
        for dataset in datasets:
            data.append ( dataset[i] )
        out.append ( np.mean(data) )
    return out



















def plot_crash():
    estimated_of_sets = []
    mean_of_sets = []
    for i in range(1,3):
        (lats, avgs) = plot_latencies('acceptor-crash-1-' + str(i) + '.log', showsteps=False, showavg=False)
        mean_of_sets.append(lats)
        estimated_of_sets.append(avgs)

    avgs = avg_datasets(mean_of_sets)
    avgs2 = avg_datasets(estimated_of_sets)
    plt.plot(avgs,
             drawstyle='steps',
             color='#999999',
             linewidth=1.0,
             label='Unbiased estimator')
    plt.plot(avgs2,
             color='red',
             linewidth=1.25,
             label='Exponentially weighted moving average')             


def plot_crash_restore():
    estimated_of_sets = []
    mean_of_sets = []
    
    for i in range(1,11):
        (lats, avgs) = plot_latencies('replica-crash-restore-' + str(i) + '.log', showsteps=False, showavg=False)
        mean_of_sets.append(lats)
        estimated_of_sets.append(avgs)

    avgs = avg_datasets(mean_of_sets)
    avgs2 = avg_datasets(estimated_of_sets)
    plt.plot(avgs,
             drawstyle='steps',
             color='#999999',
             linewidth=1.25,
             label='Unbiased estimator')
    plt.plot(avgs2,
             color='blue',
             linewidth=1.25,
             label='Exponentially weighted moving average')

def format_latency_graph():
    plt.grid(True)
    plt.ylabel( 'latency / ms' )
    plt.xlabel( 'messages sent' )

    ax = plt.gca()

    ax.set_ylim([0,100])
    ax.set_xlim([25,250])    

    # Major ticks every 20, minor ticks every 5
    major_ticks_x = np.arange(25, 250, 20)
    major_ticks_y = np.arange(0, 101, 5)
    ax.set_xticks(major_ticks_x)
    ax.set_yticks(major_ticks_y)





fig = plt.figure()
plot_latencies("bandwidth-test.log", showavg=False, stepcolor='blue')
plot_latencies("bandwidth-testo-1.log", showavg=False, stepcolor='red')
#plot_latencies("latency-testo-1.log")


fig = plt.figure()  
format_latency_graph()
# Plot the baseline
plot_latencies('acceptor-crash-base.log', avgcolor='black', showsteps=False, showavg=True)
plot_crash()
# Plot line that signifies point at which acceptor is terminated
plt.axvline(x=130, linewidth=1.0, linestyle='dashed', color='#333333')
plt.legend()



fig = plt.figure()  
format_latency_graph()
# Plot the baseline
plot_latencies('acceptor-crash-base.log', avgcolor='black', showsteps=False, showavg=True)
# Plot line that signifies point at which acceptor is terminated and restored
plot_crash_restore()
plt.axvline(x=130, linewidth=1.0, linestyle='dashed', color='#333333')
plt.axvline(x=190, linewidth=1.0, linestyle='dashed', color='#333333')
plt.legend()





#fig = plt.figure()
#plot_latencies("commit-latency/f-trace-1.log", avgcolor='red')
#plot_latencies("commit-latency/f-trace-2.log", avgcolor='blue')

#plot_latencies("bandwidth-test-1.log", avgcolor="red")
#plot_latencies("bandwidth-test-2.log", avgcolor="red")
#plot_latencies("bandwidth-test-3.log", avgcolor="red")
#plot_latencies("bandwidth-test-4.log", avgcolor="red")
#plot_latencies("bandwidth-test-5.log", avgcolor="red")
#plot_latencies("bandwidth-test-6.log", avgcolor="red")
#plot_latencies("bandwidth-test-7.log", avgcolor="red")
#plot_latencies("bandwidth-test-8.log", avgcolor="red")
#plot_latencies("bandwidth-test-9.log", avgcolor="red")
#plot_latencies("bandwidth-test-10.log", avgcolor="red")





















# Average a list of latencies
def average_latencies(latencies):
    return np.mean(latencies)

# Open a list of latency traces and average the values in each one,
# putting the result in another list
def foo(filenames):
    avg_latencies = []
    errs = []
    for filename in filenames:
        file = open(os.path.expanduser("~/share-test/" + filename), "r")
        latencies = []
        for line in file.readlines ():
            latency = float( line[0:(len(line)-1)] )
            latencies.append(latency)
        avg_latencies.append( np.mean( latencies ) ) 
        errs.append ( 1.96 * math.sqrt( np.var(latencies) ) / math.sqrt( len(latencies) ) )
    return (avg_latencies, errs)

filenames = []
for i in range(1,21):
    filenames.append("commit-latency/trace-" + str(i) + ".log")    
(results,errs) = foo(filenames)

filenames2 = []
for i in range(1,21):
    filenames2.append("commit-latency/acceptor-trace-" + str(i) + ".log")    
(results2,errs2) = foo(filenames2)

filenames3 = []
for i in range(1,21):
    filenames3.append("commit-latency/leader-trace-" + str(i) + ".log")    
(results3,errs3) = foo(filenames3)







fig, ax = plt.subplots()

# ax.set_ylim([0,75])
ax.set_yticks(np.arange(0,75.1,5))
ax.grid(True)
ax.xaxis.grid() # horizontal lines



width = 1.0

ind =  np.arange(0,60,3)  # the x locations for the groups

ax.set_xticks(ind + width)
ax.set_xticklabels( np.arange(1,21,1) )
ax.tick_params(axis=u'x', which=u'both',length=0)


rects1 = ax.bar(ind, results, width, color='#999999',yerr=errs,edgecolor="#888888", label="replicas", error_kw=dict(ecolor='#333333'))
rects2 = ax.bar(ind + width, results2, width, color='#cccccc',yerr=errs2,edgecolor="#bbbbbb", label="acceptors", error_kw=dict(ecolor='#333333'))

plt.legend()
plt.show()


