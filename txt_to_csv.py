import os
import csv


def convert_txt_to_csv(directory, delimiter='\t'):
    for filename in os.listdir(directory):
        if filename.endswith('.txt'):
            txt_file = os.path.join(directory, filename)
            csv_file = os.path.join(directory, filename.replace('.txt', '.csv'))

            # Try opening the file with utf-8 encoding, fallback to ISO-8859-1 if it fails
            try:
                with open(txt_file, 'r', encoding='utf-8') as infile:
                    lines = infile.readlines()
            except UnicodeDecodeError:
                with open(txt_file, 'r', encoding='ISO-8859-1') as infile:
                    lines = infile.readlines()

            with open(csv_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                for line in lines:
                    row = line.strip().split(delimiter)
                    writer.writerow(row)
            print(f'Converted {txt_file} to {csv_file}')


# Example usage:
directory = '/home/max/Desktop/Hiwi_Job/BON_LUH_22052024_BOE_004'
convert_txt_to_csv(directory)
