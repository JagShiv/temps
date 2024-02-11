import os
import sys
import csv
import json
import getopt
import gzip
import shutil
import pandas as pd
import paramiko as pk
from scp import SCPClient

password = "T3nx3r73c4$"
username = "pi"
board = ""
filename = ""


def run_cmd(ip):
    """Function to fetch analytics.log files from connected server and store it in folder with host name"""
    ssh = pk.SSHClient()
    ssh.set_missing_host_key_policy(pk.AutoAddPolicy())
    print(f"[ INFO ] Trying to connect {ip}")
    try:
        ssh.connect(ip, 22, username, password, timeout=5)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("hostname")
        host = ssh_stdout.readlines()
        host = host[0]
        user = os.getlogin()
        print(f"[ INFO ] Connection to IP={ip}, Hostname={host} successful")
        print(f"[ INFO ] Fetching log from {host}")
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("ls /var/log/")
        files = ssh_stdout.readlines()
        if not os.path.exists(f"C:/Users/{user}/{host.strip()}"):
            os.makedirs(f"C:/Users/{user}/{host.strip()}")
        if len(filename) > 0:
            scp = SCPClient(ssh.get_transport())
            scp.get(f"/var/log/{filename.strip()}",
                    f"C:/Users/{user}/{host.strip()}", recursive=True)
            oldname1 = os.path.basename(f"C:/Users/{user}/{host.strip()}/{filename}")
            root1, ext1 = os.path.splitext(oldname1)
            if ext1 in ['.gz', '.bz2']:
                ext1 = os.path.splitext(root1)[1] + ext1
            os.rename(f"C:/Users/{user}/{host.strip()}/{filename}",
                      f"C:/Users/{user}/{host.strip()}/{host.strip()}{ext1}")
        else:
            for file in files:
                if file.startswith("analytics.log-"):
                    for file in files:
                        if file.startswith("analytics.log-"):
                            scp = SCPClient(ssh.get_transport())
                            scp.get(f"/var/log/{file.strip()}",
                                    f"C:/Users/{user}/" f"{host.strip()}", recursive=True)
                            old_name1 = os.path.basename(f"C:/Users/{user}/"
                                                         f"{host.strip()}/{file.strip()}")
                            root, ext = os.path.splitext(old_name1)
                            if ext in ['.gz', '.bz2']:
                                ext = os.path.splitext(root)[1] + ext
                            os.rename(f"C:/Users/{user}/{host.strip()}/{file.strip()}",
                                      f"C:/Users/{user}/{host.strip()}/{host.strip()}{ext}")
                    break
            else:
                print(f"analytics not found in {host}")
        print("[ INFO ] Process Completed")
        return host, user
    except Exception as exc:
        print("[ ERR  ] Failed to connect ", ip, exc)
    ssh.close()


def json_to_csv(input_file_name, output_file_name):
    """Function to convert log file to csv file with oredered column header"""
    headers = ["time", "hostname", "stack label", "value", "user name", "uuid", "uid", "active time", "sfeature1",
               "svalue1", "sfeature2", "svalue2", "sfeature3", "svalue3", "stime", "ffeature1", "fvalue1", "ffeature2",
               "fvalue2", "ffeature3", "fvalue3", "error", "error no", "nfeature1", "nvalue1", "nfeature2", "nvalue2",
               "nfeature3", "nvalue3","nfeature4", "nvalue4","nfeature5", "nvalue5","nfeature6", "nvalue6", "message"]

    values = ["stack_label", "value", "user_name", "uuid", "uid", "active_time", "sfeature1", "svalue1", "sfeature2","svalue2",
            "sfeature3", "svalue3", "time_taken", "ffeature1", "fvalue1", "ffeature2", "fvalue2",
              "ffeature3", "fvalue3","Error", "ErrorNo", "nfeature1", "nvalue1", "nfeature2", "nvalue2", "nfeature3",
              "nvalue3","nfeature4", "nvalue4","nfeature5", "nvalue5","nfeature6","nvalue6"]

    output_file1 = open(output_file_name, "+w", newline="")
    csv_writer = csv.writer(output_file1)
    csv_writer.writerow(headers)
    try:
        with open(input_file_name, "r") as f:
            for i in f.readlines():
                i = i.strip()
                try:
                    data = json.loads(i)
                except Exception as exce:
                    if "{" in i:
                        data = json.loads(i[i.index("{"):])
                    else:
                        continue
                try:
                    message_data = json.loads(data['message'])
                except Exception as excep:
                    continue
                res_row = []
                res_row.append(data.setdefault('@timestamp', ""))
                res_row.append(data.setdefault('hostname', ""))
                for value in values:
                    res_row.append(message_data.setdefault(value, ""))
                res_row.append(data.setdefault('message', ""))
                csv_writer.writerow(res_row)
        output_file1.close()
    except Exception as excepp:
        print("[ ERR  ] Json file not created ", excepp)

def column_alignment(output_file1):
    """ row aligned properly and dropped empty cells """
    df = pd.read_csv(output_file1)
    df['user name'], df['active time'] = df['user name'].shift(-1), df['active time'].shift(-1)
    df = df.dropna(subset=['stack label', 'active time', 'value'], how='all')
    df.to_csv(output_file1)




#To extract the Zipped folders (.gz)
def extract_folder(folder_path, output_path):
    # Iterate over all files in the folder
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.gz'):
                file_path = os.path.join(root, file_name)

                # Create the output file path by removing the '.gz' extension
                output_file_path = os.path.join(output_path, file_name[:-3])

                # Open the input and output files in binary mode
                with gzip.open(file_path, 'rb') as gz_file, open(output_file_path, 'wb') as out_file:
                    # Copy the contents from the compressed file to the output file
                    shutil.copyfileobj(gz_file, out_file)
                os.remove(file_path)




# Takes argv input from user(board name and file name)
argv = sys.argv[1:]
try:
    options, args = getopt.getopt(argv, "b:f:",
                                  ["board =",
                                   "file ="])
    for name, value in options:
        if name in ['-b', '--board']:
            board = value
        elif name in ['-f', '--file']:
            filename = value
except Exception as eccepe:
    print("invalid argument ")



# run_cmd function will take board name if given, else ip address
if len(board) > 0:
    temp = run_cmd(board)
    if temp:
        directory = f"C:/Users/{temp[1]}/{temp[0].strip()}"
        for logfile in os.listdir(directory):
            root2, ext2 = os.path.splitext(logfile)
            if ext2 not in ['.gz', '.bz2']:
                input_file = f"C:/Users/{temp[1]}/{temp[0].strip()}/{logfile}"
                output_file = f"C:/Users/{temp[1]}/{temp[0].strip()}/{logfile}.csv"
                json_to_csv(input_file, output_file)
                column_alignment(output_file)
else:
    for j in range(3, 4):
        ip_add = f"192.168.0.{j}"
        temp = run_cmd(ip_add)
        # selecting log file without extension and passing as argument to json_to_csv function
        if temp:
            directory = f"C:/Users/{temp[1]}/{temp[0].strip()}"
            extract_folder(directory, directory)# comment if older files not required(if only previous month is required comment)
            # for logfile in os.listdir(directory):
            #     root2, ext2 = os.path.splitext(logfile)
                # if ext2 not in ['.gz', '.bz2']:
                #     input_file = f"C:/Users/{temp[1]}/{temp[0].strip()}/{logfile}"
                #     output_file = f"C:/Users/{temp[1]}/{temp[0].strip()}/{logfile}.csv"
                    # json_to_csv(input_file, output_file)
                    # column_alignment(output_file)
