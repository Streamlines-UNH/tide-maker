import os
import mbutil


def main():
    file = open("./MBTILE.mbtiles")
    mbtile = file.read()
    mbutil.mbtiles_to_disk(mbtile, "./tiles/")
    print("XXXXX")

main()