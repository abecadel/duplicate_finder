import os
import logging
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def info(s):
    logging.info(s)


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return (fname, hash_md5.digest())


def list_files_in_directory(dirr):
    info(f"Working directory: {dirr}")
    return [
        os.path.join(dirr, x)
        for x in os.listdir(dirr)
        if os.path.isfile(os.path.join(dirr, x))
    ]


def calculate_sums(files):
    sums = list()
    with PoolExecutor(max_workers=os.cpu_count()) as executor:
        for ret in executor.map(md5, files):
            info(f"Processed {len(sums)} out of {len(files)}")
            sums.append(ret)
    return sums


def find_duplicates(sums):
    seen = set()
    duplicates = list()
    for file, h in sums:
        if h not in seen:
            seen.add(h)
        else:
            duplicates.append(file)

    info(f"No of duplicates found: {len(duplicates)}")
    return duplicates


def write_out(duplicates, out_file):
    with open(out_file, "w") as f:
        for file in duplicates:
            f.write(file + "\n")


def find_duplicate_files(dirr):
    all_files = list_files_in_directory(dirr)
    info(f"No of files found: {len(all_files)}")
    sums = calculate_sums(all_files)
    return find_duplicates(sums)


def find_and_write_out_duplicate_files(duplicates, out):
    write_out(duplicates, out)


def remove_found_duplicates(duplicates):
    for f in duplicates:
        info(f"Removing duplicate file {f}")
        os.remove(f)
        info(f"File deleted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", help="Directory to work on", type=str, required=True)
    parser.add_argument("--out", help="Output file", type=str, required=True)
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()
    duplicates = find_duplicate_files(args.dir)
    find_and_write_out_duplicate_files(duplicates, args.out)

    if args.delete is True:
        remove_found_duplicates(duplicates)
