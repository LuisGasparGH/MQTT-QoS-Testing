import os, sys, json, zipfile, pyshark, statistics, regex
import time

# RUN THIS FILE FROM THE ROOT DIRECTORY AND NOT THE SCRIPTS DIRECTORY

variables = {
    "device": ["server", "clients"],
    "client_amount": ["1 clients", "2 clients", "5 clients", "10 clients"],
    "cpu_performance": ["100 CPU", "63 CPU", "25 CPU"],
    "tcp_algorithm": ["tcpON", "tcpOFF"],
    "queue_size": ["1000 messages", "100 messages", "10 messages"],
    "qos_level": ["QoS 0", "QoS 1", "QoS 2"],
    "payload_size": ["1250 bytes", "12500 bytes", "125000 bytes"],
    "frequency": ["5 Hz", "25 Hz", "50 Hz"],
}

def results_file_builder():
    print("[RFB] Building results file...")
    results = {}
    
    for device in variables["device"]:
        results[device] = {}
        for client_amount in variables["client_amount"]:
            results[device][client_amount] = {}
            for cpu_performance in variables["cpu_performance"]:
                results[device][client_amount][cpu_performance] = {}
                for tcp_algorithm in variables["tcp_algorithm"]:
                    results[device][client_amount][cpu_performance][tcp_algorithm] = {}
                    for queue_size in variables["queue_size"]:
                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size] = {}
                        for qos_level in variables["qos_level"]:
                            results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level] = {}
                            for payload_size in variables["payload_size"]:
                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size] = {}
                                for frequency in variables["frequency"]:
                                    results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency] = {}
                                    results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["done"] = False
                                    results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["uuid"] = ""
                                    if device == "server":
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["processed"] = False
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["loss"] = 0.0
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["timefactor"] = 0.0
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freq"] = 0.0
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqfactor"] = 0.0
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["ooomsgs"] = 0
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["rtt"] = 0.0
                                    elif device == "clients":
                                        results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc0"] = 0.0
                                        match client_amount:
                                            case "2 clients":
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc1"] = 0.0
                                            case "5 clients":
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc1"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc2"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc3"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc4"] = 0.0
                                            case "10 clients":
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc1"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc2"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc3"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc4"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc5"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc6"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc7"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc8"] = 0.0
                                                results[device][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqc9"] = 0.0
                        
    with open("scripts/dataset_results.json", "w") as results_file:
        json.dump(results, results_file, indent=4)
        results_file.close()
    print("[RFB] Results file built.")

def log_file_processor(device):
    with open("scripts/dataset_results.json", "r") as results_file:
        results = json.load(results_file)
        results_file.close()

    if device == "server":
        print("[LFP] Processing server logs...")
        execution_details_lut = {}
        server_log_list = os.listdir(f"logs/server")
        
        log_counter = 0
        for log_file_name in server_log_list:
            if "-main-" in log_file_name:
                log_counter += 1
                print(f"[LFP] [{log_counter}/{len(server_log_list)}] Processing {log_file_name}...")
                
                log_data = open(f"logs/server/{log_file_name}").read().splitlines()
        
                client_amount = ""
                cpu_performance = ""
                tcp_algorithm = ""
                queue_size = ""
                qos_level = ""
                payload_size = ""
                frequency = ""
                uuid = ""
                repetition = 0

                packet_loss = []
                time_factor = []
                actual_freq = []
                freq_factor = []
                
                for line in log_data:
                    if "Broker CPU performance" in line:
                        cpu_performance = f"{line.split(':')[-1][1:-1]} CPU"
                    elif "Max queue size per client" in line:
                        queue_size = line.split(':')[-1][1:]
                    elif "TCP no delay algorithm" in line:
                        if "True" in line: tcp_algorithm = "tcpON"
                        else: tcp_algorithm = "tcpOFF"
                    elif "REPETITION" in line:
                        repetition = int(regex.findall(r'REPETITION \d+', line)[0].split(' ')[1])
                    elif "Run UUID" in line:
                        uuid = line[-36:]
                    elif "Client amount" in line:
                        client_amount = line.split(':')[-1][1:]
                    elif "Message size" in line:
                        payload_size = line.split(':')[-1][1:]
                    elif "Publishing frequency" in line:
                        frequency = line.split(':')[-1][1:]
                    elif "QoS level: " in line:
                        qos_level = f"QoS {line.split(':')[-1][1:]}"
                    elif "Calculated packet loss" in line:
                        packet_loss.append(float(line.split(':')[-1][1:-1]))
                    elif "Time factor" in line:
                        time_factor.append(float(regex.findall(r'Time factor: \d+\.\d+', line)[0].split(':')[1][1:]))
                    elif "Actual frequency" in line:
                        actual_freq.append(float(line.split(':')[-1][1:-3]))
                    elif "Frequency factor" in line:
                        freq_factor.append(float(line.split(':')[-1][1:-1]))
                        if repetition == 10:
                            execution_details_lut[uuid] = f"{client_amount};{cpu_performance};{tcp_algorithm};{queue_size};{qos_level};{payload_size};{frequency}"
                            if results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["done"] == False:
                                print(f"Added server log data for run {uuid}")
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["done"] = True
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["uuid"] = uuid
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["loss"] = round(statistics.mean(packet_loss),3)
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["timefactor"] = round(statistics.mean(time_factor),3)
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freq"] = round(statistics.mean(actual_freq),3)
                                results["server"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["freqfactor"] = round(statistics.mean(freq_factor),3)
                            packet_loss = []
                            time_factor = []
                            actual_freq = []
                            freq_factor = []
        
        with open("scripts/execution_details_lut.json", "w") as lut_file:
            json.dump(execution_details_lut, lut_file, indent=4)
            lut_file.close()
        print("[LFP] Information added to LUT file.")

    elif device == "clients":
        with open("scripts/execution_details_lut.json", "r") as lut_file:
            execution_details_lut = json.load(lut_file)
            lut_file.close()

        for client in range(10):
            print(f"[LFP] Processing client {client} logs...")
            client_log_list = os.listdir(f"logs/client-{client}")

            log_counter = 0
            for log_file_name in client_log_list:
                if "-main-" in log_file_name:
                    log_counter += 1
                    print(f"[LFP] [{log_counter}/{len(client_log_list)}] Processing {log_file_name}...")

                    log_data = open(f"logs/client-{client}/{log_file_name}").read().splitlines()

                    client_amount = ""
                    cpu_performance = ""
                    tcp_algorithm = ""
                    queue_size = ""
                    qos_level = ""
                    payload_size = ""
                    frequency = ""
                    uuid = ""
                    invalid = False

                    freqclient = []

                    for line in log_data:
                        if "Run UUID" in line:
                            uuid = line[-36:]
                            if uuid in execution_details_lut:
                                details = execution_details_lut[uuid].split(';')
                                invalid = False
                                client_amount = details[0]
                                cpu_performance = details[1]
                                tcp_algorithm = details[2]
                                queue_size = details[3]
                                qos_level = details[4]
                                payload_size = details[5]
                                frequency = details[6]
                            else:
                                invalid = True
                        # TODO - INVESTIGATE POSSIBILITY OF BAD LOGIC HERE, AS IT MAY BE COUNTING RESULTS FROM SERVER-ONLY INVALID RUNS,
                        # SUCH AS WHEN EXECUTION TIME IS TOO LOW, AND THEREFORE MAY BE SKIPPING ACTUAL VALID RESULTS
                        # CHECK FOR RUNS IN THE DATASET WITH 11 CAPTURE FILES AND DOUBLE CHECK THE RESULTS ON THE CLIENT SIDE, AND UPDATE CODE TO AVOID THOSE
                        elif "Actual frequency (from the client)" in line and invalid == False:
                            freqclient.append(float(line.split(':')[-1][1:-3]))
                            if len(freqclient) == 10:
                                results["clients"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["done"] = True
                                results["clients"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency]["uuid"] = uuid
                                results["clients"][client_amount][cpu_performance][tcp_algorithm][queue_size][qos_level][payload_size][frequency][f"freqc{client}"] = round(statistics.mean(freqclient),3)
                                freqclient = []

    with open("scripts/dataset_results.json", "w") as results_file:
        json.dump(results, results_file, indent=4)
        results_file.close()
    print("[LFP] Log results added to results file.")

def zip_organizer():
    print("[ZO] Organizing zip files...")
    with open("scripts/execution_details_lut.json", "r") as lut_file:
        execution_details_lut = json.load(lut_file)
        lut_file.close()

    device_folder_list = os.listdir("dumpcap/server")
    for client_amount in device_folder_list:
        zip_list = os.listdir(f"dumpcap/server/{client_amount}")
        print(f"[ZO] Creating folders for {client_amount}...")
        os.makedirs(f"dumpcap/server/{client_amount}/QoS 0", exist_ok=True)
        os.makedirs(f"dumpcap/server/{client_amount}/QoS 1", exist_ok=True)
        os.makedirs(f"dumpcap/server/{client_amount}/QoS 2", exist_ok=True)
        os.makedirs(f"dumpcap/server/{client_amount}/invalid", exist_ok=True)
        
        zip_counter = 0
        for zip_file in zip_list:
            if ".zip" in zip_file:
                zip_counter += 1
                uuid = zip_file.split('U')[-1][:-4]

                if uuid in execution_details_lut:
                    qos = execution_details_lut[uuid].split(';')[4]
                    
                    rezip = False
                    print(f"[ZO] [{zip_counter}/{len(zip_list)}] Checking {zip_file} for duplicate executions...")
                    with zipfile.ZipFile(f"dumpcap/server/{client_amount}/{zip_file}", 'r') as open_zip:
                        file_list = open_zip.namelist()
                        
                        if len(file_list) > 10:
                            rezip = True
                            previous_filename = ""
                            previous_repetition = ""

                            for filename in file_list:
                                repetition = filename.split('-')[6]
                                if repetition == previous_repetition:
                                    delete = previous_filename
                                    break
                                previous_filename = filename
                                previous_repetition = repetition
                            
                            print(f"[ZO] {zip_file} has more than 10 capture files. Removing {delete}...")
                            open_zip.extractall(f"dumpcap/server/{client_amount}/unzipped")
                            os.remove(f"dumpcap/server/{client_amount}/unzipped/{delete}")
                            open_zip.close()
                            
                    if rezip == True:
                        os.remove(f"dumpcap/server/{client_amount}/{zip_file}")
                        file_list = os.listdir(f"dumpcap/server/{client_amount}/unzipped")

                        with zipfile.ZipFile(f"dumpcap/server/{client_amount}/{zip_file}", 'a', zipfile.ZIP_DEFLATED) as open_zip:
                            for file in file_list:
                                open_zip.write(f"dumpcap/server/{client_amount}/unzipped/{file}", file)
                                os.remove(f"dumpcap/server/{client_amount}/unzipped/{file}")
                            open_zip.close()
                        print(f"[ZO] {zip_file} reorganized.")
                        os.rmdir(f"dumpcap/server/{client_amount}/unzipped")

                    print(f"[ZO] [{zip_counter}/{len(zip_list)}] Moving {zip_file} to respective folder...")
                    try:
                        os.rename(f"dumpcap/server/{client_amount}/{zip_file}", f"dumpcap/server/{client_amount}/{qos}/{zip_file}")
                    except FileExistsError:
                        print(f"[ZO] [{zip_counter}/{len(zip_list)}] Duplicate {zip_file} in destination folder, ignoring...")
                else:
                    try:
                        os.rename(f"dumpcap/server/{client_amount}/{zip_file}", f"dumpcap/server/{client_amount}/invalid/{zip_file}")
                    except FileExistsError:
                        print(f"[ZO] [{zip_counter}/{len(zip_list)}] Duplicate {zip_file} in destination folder, deleting...")
                        os.remove(f"dumpcap/server/{client_amount}/{zip_file}")

def remaining_run_checker():
    print(f"[RRC] Checking results file for missing runs...")
    with open("scripts/dataset_results.json", "r") as results_file:
        results = json.load(results_file)
        results_file.close()
    
    for v1 in variables["client_amount"]:
        for v2 in variables["cpu_performance"]:
            for v3 in variables["tcp_algorithm"]:
                for v4 in variables["queue_size"]:
                    for v5 in variables["qos_level"]:
                        for v6 in variables["payload_size"]:
                            for v7 in variables["frequency"]:
                                done = results["server"][v1][v2][v3][v4][v5][v6][v7]["done"]
                                processed = results["server"][v1][v2][v3][v4][v5][v6][v7]["processed"]

                                if done and not processed:
                                    print(f"[RRC] Run with details {v1};{v2};{v3};{v4};{v5};{v6};{v7} done but not processed yet")
                                # if not done and not processed:
                                #     print(f"[RRC] Run with details {v1};{v2};{v3};{v4};{v5};{v6};{v7} not done yet")

def pcap_file_processor(clients, qos):
    print(f"[PFP] Processing {clients} {qos} capture files...")
    with open("scripts/dataset_results.json", "r") as results_file:
        results = json.load(results_file)
        results_file.close()
    with open("scripts/execution_details_lut.json", "r") as lut_file:
        execution_details_lut = json.load(lut_file)
        lut_file.close()

    zip_list = os.listdir(f"dumpcap/server/{clients}/{qos}")

    zip_counter = 0
    unzipped_folder = False
    for zip_file in zip_list:
        if "server-" in zip_file:
            zip_counter += 1
            print(f"[PFP] [{zip_counter}/{len(zip_list)}] Processing {zip_file}...")
        
            uuid = zip_file.split('U')[-1][:-4]
            details = execution_details_lut[uuid].split(';')

            if results["server"][details[0]][details[1]][details[2]][details[3]][details[4]][details[5]][details[6]]["processed"] == False:
                os.makedirs(f"dumpcap/server/{clients}/{qos}/unzipped", exist_ok=True)
                unzipped_folder = True

                with zipfile.ZipFile(f"dumpcap/server/{clients}/{qos}/{zip_file}", 'r') as zip_file:
                    zip_file.extractall(f"dumpcap/server/{clients}/{qos}/unzipped")
                    zip_file.close()

                ooo_messages = 0
                rtt = []

                capture_list = os.listdir(f"dumpcap/server/{clients}/{qos}/unzipped")
                capture_counter = 0
                for capture_file in capture_list:
                    capture_counter += 1
                    print(f"[PFP] [{capture_counter}/{len(capture_list)}] Processing {capture_file}...")

                    last_message_number = {}
                    ports = {}
                    messages = {}

                    capture = pyshark.FileCapture(f"dumpcap/server/{clients}/{qos}/unzipped/{capture_file}")
                    capture.load_packets()
                    packet_counter = 0
                    for packet in capture:
                        packet_counter += 1
                        print(f"[PFP] Processing packet {packet_counter}...", end='\r')
                        match details[4]:
                            case "QoS 0":
                                if 'analysis_ack_rtt' in dir(packet.tcp):
                                    rtt.append(float(packet.tcp.analysis_ack_rtt)*1000)
                            case "QoS 1" | "QoS 2":
                                if 'mqtt' in dir(packet):
                                    match packet.mqtt.msgtype:
                                        case '3':
                                            if 'topic' in dir(packet.mqtt):
                                                if 'main_topic' in packet.mqtt.topic:
                                                    client_id = packet.mqtt.topic.split('/')[-1]
                                                    if 'srcport' in dir(packet.tcp):
                                                        if packet.tcp.srcport not in ports:
                                                            try:
                                                                ports[packet.tcp.srcport] = client_id
                                                            except:
                                                                print(f"[PFP] Error: {packet}")
                                                        
                                                        if client_id not in messages:
                                                            messages[client_id] = {}
                                                        
                                                        messages[client_id][packet.mqtt.msgid] = float(packet.sniff_timestamp)
                                        case '4' | '7':
                                            if 'dstport' in dir(packet.tcp):
                                                if packet.tcp.dstport in ports:
                                                    if packet.mqtt.msgid in messages[ports[packet.tcp.dstport]]:
                                                        rtt.append(float(float(packet.sniff_timestamp) - messages[ports[packet.tcp.dstport]][packet.mqtt.msgid])*1000)
                            
                        if 'mqtt' in dir(packet) and 'topic' in dir(packet.mqtt):	
                                if packet.mqtt.msgtype == '3' and 'main_topic' in packet.mqtt.topic:
                                    client = packet.mqtt.topic.split('/')[-1]
                                    if client not in last_message_number:
                                        last_message_number[client] = 0

                                    msg_num = packet.mqtt.msg.hex_value+1
                                    if msg_num > last_message_number[client]:
                                        last_message_number[client] = msg_num
                                    else:
                                        ooo_messages += 1
                    
                    capture.close()
                    os.remove(f"dumpcap/server/{clients}/{qos}/unzipped/{capture_file}")

                with open("scripts/dataset_results.json", "r") as results_file:
                    results = json.load(results_file)
                    results_file.close()
                
                results["server"][details[0]][details[1]][details[2]][details[3]][details[4]][details[5]][details[6]]["processed"] = True
                results["server"][details[0]][details[1]][details[2]][details[3]][details[4]][details[5]][details[6]]["ooomsgs"] = ooo_messages
                results["server"][details[0]][details[1]][details[2]][details[3]][details[4]][details[5]][details[6]]["rtt"] = round(statistics.mean(rtt),3)

                with open("scripts/dataset_results.json", "w") as results_file:
                    json.dump(results, results_file, indent=4)
                    results_file.close()
                print("[PFP] Capture file results added to results file.")
            else:
                print(f"[PFP] {zip_file} already processed. Skipping...")
    
    if unzipped_folder == True:
        os.rmdir(f"dumpcap/server/{clients}/{qos}/unzipped")

if len(sys.argv) == 1:
    print(f"[DP] Executing step 1")
    results_file_builder()
    log_file_processor("server")
    log_file_processor("clients")
    zip_organizer()
elif len(sys.argv) == 3:
    print(f"[DP] Executing step 2")
    pcap_file_processor(f"{sys.argv[1]}C", f"QoS {sys.argv[2]}")
elif len(sys.argv) == 2 and sys.argv[1] == "rrc":
    print(f"[DP] Executing step 3")
    remaining_run_checker()