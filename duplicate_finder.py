import os
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor as PoolExecutor


def log(s):
    print(s)


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return (fname, hash_md5.digest())


def list_files_in_directory(dirr):
    log(f"Working directory: {dirr}")
    return [
        os.path.join(dirr, x)
        for x in os.listdir(dirr)
        if os.path.isfile(os.path.join(dirr, x))
    ]


def calculate_sums(files):
    sums = list()
    with PoolExecutor(max_workers=os.cpu_count()) as executor:
        for ret in executor.map(md5, files):
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
    return duplicates


def write_out(duplicates, out_file):
    with open(out_file, "w") as f:
        for file in duplicates:
            f.write(file + "\n")


def main(dirr, out):
    all_files = list_files_in_directory(dirr)
    sums = calculate_sums(all_files)
    duplicates = find_duplicates(sums)
    write_out(duplicates, out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", help="Directory to work on", type=str)
    parser.add_argument("out", help="Output file", type=str)
    args = parser.parse_args()
    main(args.dir, args.out)
