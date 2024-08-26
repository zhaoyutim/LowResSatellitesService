import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--string1', default="Hello world!")
parser.add_argument('--string2', default="Bye")
args = parser.parse_args()

print(args.string1)
import time

time.sleep(10)

print(args.string2)
