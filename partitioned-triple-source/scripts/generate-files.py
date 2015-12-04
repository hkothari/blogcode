import csv
import random
import string

NUM_POINTS=1000

def generate_data(key):
    print("Generating data for: " + key)
    with open("outputdir/" + key + ".csv", 'w') as key_file:
        writer = csv.writer(key_file)
        for i in range(0, NUM_POINTS):
            writer.writerow([key, i, random.random()])

if __name__ == '__main__':
    for a in string.ascii_uppercase:
        for i in range(1, 10):
            generate_data(a + str(i))
