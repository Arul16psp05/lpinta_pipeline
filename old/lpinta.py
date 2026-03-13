#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 11:49:18 2022
Edited on Wed Aug 24 12pm
Edited on 07_03_2024
Version Lpinta_07_03_2024.py
@author: arul
"""

import os, grp, time
import numpy as np
import argparse
import subprocess
import shutil


# Instantiate the parser
parser = argparse.ArgumentParser(description='Lpinta description')
parser.add_argument('--test', action='store_true', help='Test on file availability and permissions')
parser.add_argument('--nodel', action='store_true', help='To keep processed data files')
parser.add_argument('--pardir', type=str, default=None, help='Pulsar parameter file directory')
parser.add_argument('input_dir', type=str, help= 'Input data directory')
parser.add_argument('working_dir', type=str, help= 'Working directory')

args = parser.parse_args()

basepath = args.input_dir	#/Data/prabu/FORAPANDIAN/INDPTA001/fullData
workDir = args.working_dir    
pardir = args.pardir
#pardir=None
pipeline_file = (os.getcwd()+"/Lpipeline.in")
app_list = ['pyGSB2DAT', 'dspsr', 'psredit', 'pdmp', 'ps2pdf']

file_list = np.genfromtxt(pipeline_file, dtype=str)

class parameters:
    def __init__(self, file_list, pardir, basepath, workDir):
        self.SourceName = file_list[0]
        self.timeStamp = file_list[1]
        self.filename = self.timeStamp.rsplit('.timestamp')[0]
        self.rcp1 = str(self.filename+'.Pol-R1.dat')
        self.rcp2 = str(self.filename+'.Pol-R2.dat')
        self.lcp1 = str(self.filename+'.Pol-L1.dat')
        self.lcp2 = str(self.filename+'.Pol-L2.dat')
        self.frequency = str(file_list[2])
        self.nchan = int(file_list[3])
        self.int_time = str(file_list[4])
        self.nbins = str(file_list[5])
        self.fil_chan = str(str(file_list[6])+':D')
        self.cuda = str('0,1')
        self.cpu = str('0,1,2,3,4,5')
        self.thread = str('16')
        self.npol = str('4')
        self.nbit = str('8')
        self.mjd = str(self.rcp1.rsplit('.')[1])
        self.output = str(self.SourceName+"_"+self.mjd+"_"+self.frequency+".Lnorfix")
        
        #Reading parfile 
        def fetch_par_info(parfile_name):
            with open(parfile_name, 'r') as par_file:
                par_lines = par_file.readlines()
                par_tokens = dict([list(filter(lambda x: len(x)>0,line.split(' ')))[:2]for line in par_lines ])
                
                fetch_par_info.dm = par_tokens["DM"]
                if "RAJ" in par_tokens and "DECJ" in par_tokens:
                    fetch_par_info.raj = par_tokens["RAJ"]
                    fetch_par_info.decj = par_tokens["DECJ"]
                    fetch_par_info.RA_DEC = fetch_par_info.raj+fetch_par_info.decj
                else:
                    print("[ERROR] Unable to read coordinates from par file. Setting 00:00:00+00:00:00.")
                    return "00:00:00","+00:00:00"
                    
        if pardir == None:
            parfile = str("/home/inpta/pardir/"+ self.SourceName +'.par')        #Default location
            fetch_par_info(parfile)
            dm = str(fetch_par_info.dm)
            RA_DEC = str(fetch_par_info.RA_DEC)
        else:
            parfile = str(str(pardir) +'/' + self.SourceName +'.par')   #Custom location
            fetch_par_info(parfile)
            dm = str(fetch_par_info.dm)
            RA_DEC = str(fetch_par_info.RA_DEC)
        #print('parfile = '+ parfile)
        self.dm = dm
        self.RA_DEC = RA_DEC
        self.parfile = parfile
        
        def find(parentFolder, fname, workDir):
            def result(status, path, fname):
                if status == True:
                    print("file exist "+path)
                    self.cheack_file.append(path)
                    self.rmv_file.append(workDir+'/'+fname)
                elif status == False:
                    print(str(fname)+" is not exist")
                    print("[Quiting]")
                    
            status = False
            path = ""       
            for dirName, subdirs, fileList in os.walk(parentFolder):
                #print('Scanning %s...' % dirName)
                for filename in fileList:
                    #print(filename)
                    if filename == fname:
                        path = os.path.join(dirName, filename)
                        status = True
                        return result(status, path, fname)
                    else:
                        status = False
            return result(status, path, fname)
                
        self.reqFiles= [self.timeStamp, self.rcp1, self.rcp2, self.lcp1, self.lcp2]    
        self.cheack_file, self.rmv_file= [],[]
        for fname in self.reqFiles:
            #print(fname)
            find(basepath, fname, workDir)
            
        singularity_container = 'singularity exec /home/inpta/pschive_py3.sif '
        self.pyGSB2DAT = ('pyGSB2DAT -r1 '+self.rcp1+ ' -r2 '+self.rcp2+ ' -l1 '+self.lcp1+ ' -l2 '+self.lcp2+ ' -t '+self.timeStamp+ ' -S '+self.SourceName+ ' -n '+str(self.nchan)+ ' -f '+str(self.frequency)+ ' -o '+self.output)
        #print(self.gsb2dat)
        self.input_data = self.output+'.dat'
        self.dspsr = (singularity_container+'dspsr '+self.input_data+' -cpu '+self.cpu+ ' -E '+self.parfile+ ' -L '+self.int_time+ ' -b '+self.nbins+ ' -F '+self.fil_chan+ ' -N '+self.SourceName+ ' -A -e .fits -O '+self.output)
        #print(self.dspsr)
        self.pdmp_input = self.output+'.fits'
        self.psredit = (singularity_container+'psredit -c name='+self.SourceName+',be:name=GSB,coord='+self.RA_DEC+' -m '+self.pdmp_input)
        self.pdmp = (singularity_container+ 'pdmp -g '+self.output+'.ps/cps '+self.pdmp_input)
        #print(self.pdmp)
        self.ps2pdf_input = (self.output+'.ps')
        self.ps2pdf = (singularity_container+'ps2pdf '+ self.ps2pdf_input+' '+ self.output +'.pdf')
        #print(self.ps2pdf)
        

if file_list.ndim == 1:
    entry = 1
    file_list = np.vstack([file_list,file_list])
else:
    entry = file_list.shape[0]


m=1            
for i in range(entry):
    print('Pulsar '+str(m))
    globals()['pulsar%s' % m] = parameters(file_list[i], pardir, basepath, workDir) 
    m+=1
    
#process_list = ['dspsr']

def file_permission_check(fileck):
    read = os.access(fileck, os.R_OK) # Check for Read access
    size = os.stat(fileck).st_size
    if (size > 1) == True and read == True:
        print(fileck+"     Read permission.... [OK]")
        file_permission_check.exit_process = 0
    else:
        print(fileck+"     Read permission.... [NO]")
        print("[EXITING]")
        
        file_permission_check.exit_process = 1

def app_permission_check(app):
    print("[CHECK] checking for Apps...  ")
    for j in app:
        if shutil.which(j):
            ap_path = shutil.which(j)
            #Check for Existance, Read and Execulable Access
            if os.access(ap_path, os.F_OK | os.R_OK | os.X_OK) is True:
                print(" ["+j+"]"+" is available... "+ap_path)
                app_permission_check.exit_process = 0
            else:
                print("Required apps are not available")
        elif not shutil.which(j):
            print(j+ " is NOT AVAILABLE")
            print("[EXITING]")         
            app_permission_check.exit_process = 1
            
#Check group permission for all files specified in pipeline           
def group_permission(file_path):
    stat_info = os.stat(file_path)
    gid = stat_info.st_gid
    file_grp = grp.getgrgid(gid)[0]
    gid = os.getgid()
    group_info = grp.getgrgid(gid)
    user_grp = group_info.gr_name
    if file_grp == user_grp:
        print("group permission ...[OK]")
        group_permission.exit_process = 0
    else:
        print("group permission ...[NO]")
        print("[EXITING]")
        
        group_permission.exit_process = 1   

def data_process():
    os.chdir(workDir)
    for i in range(1,len(file_list)+1):
        print('')
        print('['+str(eval("pulsar"+str(i)+'.'+'SourceName'))+']')
        check_files = eval("pulsar"+str(i)+'.'+'cheack_file')
        rmv_file = eval("pulsar"+str(i)+'.'+'rmv_file')
        output = eval("pulsar"+str(i)+'.'+'output')
        symlink(check_files, rmv_file)
        #gpumem_check()  # -------------------------- Not required
        for j in app_list:
            if len(eval("pulsar"+str(i)+'.'+'cheack_file')) == 5:
                #print (eval("pulsar"+str(i)+'.'+str(j)))
                cmd = eval("pulsar"+str(i)+'.'+str(j))
                #cmd = "'"+str(j)+"'"
                os.system(cmd)
                print(cmd)
            else:
                print('['+str(j)+'] [Skipping] Data file is missing.....')
                pass
        remove(rmv_file, output)

print('')

def test_permissions():
    for i in range(1,len(file_list)+1):
        print('')
        print('['+str(eval("pulsar"+str(i)+'.'+'SourceName'))+']')
        app_permission_check(app_list)                  #calling app_permission_check function
        if len(eval("pulsar"+str(i)+'.'+'cheack_file')) == 5:
            check_files = eval("pulsar"+str(i)+'.'+'cheack_file')
            for j in range(0,len(check_files)):
                file_permission_check(check_files[j])   #calling file_permission_check function
                group_permission(check_files[j])        #calling group_permission function
                #print(check_files[j])
        else:
            file_permission_check.exit_process = 1
            print('[Skipping] This pulsar will be skipped from reduction process')
        if file_permission_check.exit_process == app_permission_check.exit_process == group_permission.exit_process == 0:
            test_permissions.exit_process = 0
        else:
            test_permissions.exit_process = 1
        
        #print(test_permissions.exit_process)
        
#making symbolic link of source files in work directory
def symlink(check_files, rmv_file):
    if len(check_files) == 5:
        for n in range(0, len(check_files)):
            process = os.symlink(check_files[n], rmv_file[n])
            if process == None:
                process = 0
            if process == 0:
                print(check_files[n]+" linked")
            else:
                print(check_files[n]+"is NOT linked")
        print("symlink process finished")
    else:
        print('[Not linked]')

#gpu memory availability check to perform gsb2dat < Required memory is 3000Mb >
def gpumem_check():
    n = np.array(((subprocess.check_output(["nvidia-smi","--query-gpu=memory.free","--format=csv,noheader,nounits"])).decode().split()),dtype = int)
    for x in n:                    
        if x > 3000:
            gpumem_check.status = True;
            print("GPU memory is Avilable")
        else:                               
            gpumem_check.status = False;
    print("Waiting for GPU memory", end = "\r")
    time.sleep(2)

def mem_check():
    while True:
        gpumem_check()
        if gpumem_check.status == True:
            break

def remove(rmv_file, output):
    if args.nodel is False:
        for rfile in range(0, len(rmv_file)):
            print(rmv_file[rfile])
            os.remove(rmv_file[rfile])
        os.remove(workDir+'/'+output+'.dat')
        os.remove(workDir+'/'+output+'.hdr')
        os.remove(workDir+'/'+output+'.ps')
        print("Remove process finished") 

#test = True
test = False
if test is True:             # will initiating test
    print("[CHECK].... checking file permissions for " )
    test_permissions()
    print("[END]... Test over")
    if test_permissions.exit_process == 0:
        print("Requirments Satisfied for.  is Ready to process")    
    elif test_permissions.exit_process != 0: 
        print("Fulfil the requirments!")    
elif test is False:         # will perform test before execution
    test_permissions()
    if test_permissions.exit_process == 0:
        data_process()
    elif test_permissions.exit_process != 0: 
        print("Fulfil the requirments!")
        #exit()
print('[END]')
