import json
import os
import regex
import statistics
import threading
import time

lut_file = open(os.path.join(os.path.dirname(__file__), 'run_data_lut.json')).read()
data_lut = json.loads(lut_file)

results_file = open(os.path.join(os.path.dirname(__file__), 'run_results.json')).read()
run_results = json.loads(results_file)

def server_log_processor():
    log_folder = 'logs/server'
    
    filenum = 0
    for file in os.listdir(log_folder):
        if "-main-" in file:
            filenum += 1
            print(f"Processing {file} [{filenum}/{len(os.listdir(log_folder))}]")
            log_file = os.path.join(log_folder, file)
            
            log_lines = open(log_file).read().splitlines()
    
            cpu_perf = ""
            queue_size = ""
            tcp = ""
            run_rep = 0
            uuid = ""
            client_amount = ""
            message_size = ""
            pub_freq = ""
            qos = ""

            packet_loss = []
            time_factor = []
            actual_freq = []
            freq_factor = []
            
            for line in log_lines:
                if "Broker CPU performance" in line:
                    cpu_perf = f"{line.split(':')[-1][1:-1]} CPU"
                if "Max queue size per client" in line:
                    queue_size = line.split(':')[-1][1:]
                elif "TCP no delay algorithm" in line:
                    if "True" in line: tcp = "tcpON"
                    else: tcp = "tcpOFF"
                elif "REPETITION" in line:
                    run_rep = int(regex.findall(r'REPETITION \d+', line)[0].split(' ')[1])
                elif "Run UUID" in line:
                    uuid = line[-36:]
                elif "Client amount" in line:
                    client_amount = line.split(':')[-1][1:]
                elif "Message size" in line:
                    message_size = line.split(':')[-1][1:]
                elif "Publishing frequency" in line:
                    pub_freq = line.split(':')[-1][1:]
                elif "QoS level: " in line:
                    qos = f"QoS {line.split(':')[-1][1:]}"
                elif "Calculated packet loss" in line:
                    packet_loss.append(float(line.split(':')[-1][1:-1]))
                elif "Time factor" in line:
                    time_factor.append(float(regex.findall(r'Time factor: \d+\.\d+', line)[0].split(':')[1][1:]))
                elif "Actual frequency" in line:
                    actual_freq.append(float(line.split(':')[-1][1:-3]))
                elif "Frequency factor" in line:
                    freq_factor.append(float(line.split(':')[-1][1:-1]))
                    if run_rep == 10:
                        # data_lut[uuid] = f"{client_amount};{cpu_perf};{tcp};{queue_size}"
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["uuid"] = []
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["uuid"].append(uuid)
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["loss"] = round(statistics.mean(packet_loss),3)
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["timefactor"] = round(statistics.mean(time_factor),3)
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["freq"] = round(statistics.mean(actual_freq),3)
                        run_results["server"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["freqfactor"] = round(statistics.mean(freq_factor),3)
                        packet_loss = []
                        time_factor = []
                        actual_freq = []
                        freq_factor = []

def client_log_processor():
    for folder in os.listdir('logs'):
        if "client-" in folder:
            filenum = 0
            log_folder = f'logs/{folder}'
            client_number = folder.split('-')[1]
            for file in os.listdir(log_folder):
                if "-main-" in file:
                    filenum += 1
                    print(f"Processing {file} [{filenum}/{len(os.listdir(log_folder))}]")
                    log_file = os.path.join(log_folder, file)

                    log_lines = open(log_file).read().splitlines()

                    uuid = ""
                    client_amount = ""
                    cpu_perf = ""
                    tcp = ""
                    queue_size = ""
                    message_size = ""
                    qos = ""
                    pub_freq = ""

                    freq_client = []

                    for line in log_lines:
                        if "Run UUID" in line:
                            uuid = line[-36:]
                            try:
                                run_data = data_lut[uuid].split(';')
                                invalid_run = False
                                client_amount = run_data[0]
                                cpu_perf = run_data[1]
                                tcp = run_data[2]
                                queue_size = run_data[3]
                            except:
                                invalid_run = True
                        elif "Message size:" in line and invalid_run == False:
                            message_size = line.split(':')[-1][1:]
                        elif "QoS level:" in line and invalid_run == False:
                            qos = f"QoS {line.split(':')[-1][1:]}"
                        elif "Publish frequency:" in line and invalid_run == False:
                            pub_freq = line.split(':')[-1][1:]
                        elif "Actual frequency (from the client)" in line and invalid_run == False:
                            freq_client.append(float(line.split(':')[-1][1:-3]))
                            if len(freq_client) == 10:
                                run_results["clients"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["uuid"] = []
                                run_results["clients"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq]["uuid"].append(uuid)
                                run_results["clients"][client_amount][cpu_perf][tcp][queue_size][qos][message_size][pub_freq][f"freq_c{client_number}"] = round(statistics.mean(freq_client),3)
                                freq_client = []

server_log_processor()
client_log_processor()

with open(os.path.join(os.path.dirname(__file__), 'run_results.json'), 'w') as json_file:
        json.dump(run_results, json_file, indent=4)
# with open(os.path.join(os.path.dirname(__file__), 'run_data_lut.json'), 'w') as json_file:
#         json.dump(data_lut, json_file, indent=4)