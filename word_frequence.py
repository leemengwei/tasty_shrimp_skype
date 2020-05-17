import os,sys,time
import pandas as pd
from IPython import embed
import re
import glob
import tqdm
from extract_msg import Message
import textract
import olefile
import xlrd
import yaml
from collections import Counter
import numpy as np
import argparse
import shutil
from time_counter import calc_time
import pickle
import weekly_bonding
import matplotlib.pyplot as plt

msg_contents = []

if __name__ == '__main__':
    #Config:
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", '--DEBUG', action='store_true', default=False)
    parser.add_argument("-S", '--FROM_SCRATCH', action='store_true', default=False)
    parser.add_argument("-Q", '--QUICK_COMPANY_DATA', action='store_true', default=False)
    args = parser.parse_args()
    FROM_SCRATCH = args.FROM_SCRATCH
    DEBUG = args.DEBUG
    DATA_PATH_PREFIX = './data/data_bonding_net/'
    msg_files = glob.glob(DATA_PATH_PREFIX+"/msgs/*.msg")
    msg_files.sort()
    for this_msg_file in tqdm.tqdm(msg_files):
        msg_sender, msg_subject, msg_content = weekly_bonding.parse_msg(args, this_msg_file)
        if not isinstance(msg_content,bool):
            msg_content = msg_content.upper()
            for token in [',', '.', '?', '!', '=', '$', '@', '#', '&', '%', '<', '>', '|', '{', '}', '[', ']', '_', '\n', '\r', '\t', ':', '(', ')', '-', '~', '/', '\\', "'", '"', '*', ';', '“', '”', '+', '‘', '’', '`']:
                msg_content = msg_content.replace(token, ' ')
                msg_content = msg_content.replace("\xa0",' ')
                msg_content = msg_content.replace("\x00",' ')
                pass
            msg_contents += msg_content.split(' ')

    words = list(set(msg_contents))
    words.sort()
    words_freq = {}
    for idx, word in enumerate(words):
        if len(re.findall('[0-9]', word))>0:continue
        count = msg_contents.count(word)
        if count>1:
            #words_freq[word] = np.log10(count)
            words_freq[word] = count/len(msg_files)

    #DataFrame and SORT
    words_freq = pd.DataFrame(data=list(words_freq.values()) , index=list(words_freq.keys()))
    words_freq = words_freq.sort_values(0).drop('')
   
    words_freq.plot(kind='bar') 
    plt.show()
    embed()


