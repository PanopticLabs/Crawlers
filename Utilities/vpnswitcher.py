import subprocess
import random
import re

#string = os.system('nmcli con').read()
#connections = string.split('\n')
#print connections[0]

#print subprocess.call(['nmcli con'])

def getVPNs():
    vpns = []
    proc = subprocess.Popen(['nmcli con'], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    #print out
    connections = out.split('\n')
    for c in connections:
        c = re.sub(r' {3,}', '  ', c)
        conn = c.split('  ')
        #print conn
        try:
            conn_type = conn[2]
            #print conn_type
            if conn_type == 'vpn':
                #print conn[0]
                vpns.append(conn[0])

        except:
            pass

    return vpns

def switchVPN(vpn):
    removeVPN()

    new_conn = '"' + vpn + '"'

    proc = subprocess.Popen(['nmcli con up id ' + new_conn], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()

    print out
    print 'New Connection: ' + new_conn


def randomVPN(vpn_list):
    try:
        old_conn = removeVPN()
        vpn_list.remove(old_conn)
    except:
        pass

    new_conn = '"' + random.choice(vpn_list) + '"'

    proc = subprocess.Popen(['nmcli con up id ' + new_conn], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()

    print out
    print 'New Connection: ' + new_conn

def removeVPN():
    conn = ''
    proc = subprocess.Popen(['nmcli'], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    #print out
    if 'VPN connection' in out:
        try:
            conn = '"' + out.split(' VPN connection\n')[0] + '"'
            print 'Old Connection: ' + conn
            proc = subprocess.Popen(['nmcli con down id ' + conn], stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            print out

        except Exception as e:
            raise

    else:
        print 'No VPN currently running!'

    return conn
