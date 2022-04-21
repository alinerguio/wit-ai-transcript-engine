import os
import sys
import datetime
import pandas as pd
from wit import Wit
from wit.wit import WitError


def log_time_specifics(start_time, end_time, dataset, quantity_files, quantity_files_fail):
    process_time = end_time - start_time

    if os.path.isfile('execution_time_specifics.txt'):
        f = open('execution_time_specifics.txt', 'a')
    else:
        f = open('execution_time_specifics.txt', 'w')
        
    f.write('\n' + dataset + ';' +  str(quantity_files) + ';' +  str(quantity_files_fail) + ';' + str(process_time))
    f.close()


def iterate_folder(client, data_dir):
    folders = os.listdir(data_dir)
    folders = [folder for folder in folders if '.' not in folder]

    if not(os.path.isdir('./transcriptions/')):
        os.mkdir('./transcriptions/')

    for folder in folders:
        start_time = datetime.datetime.now()
        quantity_files = 0 
        quantity_files_fail = 0 

        curr_dir = data_dir + folder + '/'
        all_files = [file for file in os.listdir(curr_dir) if '.wav' in file]
        output_path = './transcriptions/' + folder + '.csv'

        if os.path.isfile(output_path):
            files_transcripted = pd.read_csv(output_path)
            files_transcripted_list = files_transcripted.file.tolist()

            all_files = [file for file in all_files if file not in files_transcripted_list]
        
        try:
            for file in all_files:

                try:
                    resp = transcribe(client, curr_dir + file)
                    
                    final_result = pd.DataFrame([{'transcriptions': resp, 'file': file, 'database': folder}])

                    output_path = './transcriptions/' + folder + '.csv'
                    final_result.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
                    quantity_files += 1
                except WitError as err:
                    print('Need to retry to transcribe file ' + file + '. Error: ' + str(err))
                    quantity_files_fail += 1

        except Exception as e:
            print('STOPED data from folder ' + folder + ' because of ' + str(e))
            end_time = datetime.datetime.now()
            log_time_specifics(start_time, end_time, folder, quantity_files, quantity_files_fail)

        except KeyboardInterrupt:
            print('\nKeyboardInterrupt: stopping manually')
            end_time = datetime.datetime.now()
            log_time_specifics(start_time, end_time, folder, quantity_files, quantity_files_fail)

            sys.exit()

        end_time = datetime.datetime.now()
        log_time_specifics(start_time, end_time, folder, quantity_files, quantity_files_fail)


def transcribe(client, file):
    resp = None

    with open(file, 'rb') as f:
      resp = client.speech(f, {'Content-Type': 'audio/wav'})

    return str(resp['text'])


def log_time(start_time, end_time):
    process_time = end_time - start_time

    if os.path.isfile('execution_time.txt'):
        f = open('execution_time.txt', 'a')
    else:
        f = open('execution_time.txt', 'w')

    f.write('\n' + str(process_time))
    f.close()


if __name__ == '__main__':
    access_token = None
    data_dir = '../data/'

    if access_token == None:
        if len(sys.argv) != 2:
            print('usage: python ' + sys.argv[0] + ' <wit-token>')
            exit(1)
        access_token = sys.argv[1]

    client = Wit(access_token=access_token)
    start_time = datetime.datetime.now()

    try:
        iterate_folder(client, data_dir)
        end_time = datetime.datetime.now()
        log_time(start_time, end_time)
    except Exception as e:
        end_time = datetime.datetime.now()
        log_time(start_time, end_time)
        print(e)