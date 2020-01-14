import argparse
import hashlib
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from typing import DefaultDict, List, Tuple


def debug(s) -> None:
    logging.debug(s)


def info(s) -> None:
    logging.info(s)


def calculate_md5(fname) -> Tuple[str, bytes]:
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return fname, hash_md5.digest()


def list_files_in_directory(dirr) -> List[str]:
    debug(f"Working directory: {dirr}")
    return [
        os.path.join(dirr, x)
        for x in os.listdir(dirr)
        if os.path.isfile(os.path.join(dirr, x))
    ]


def calculate_checksums(files) -> DefaultDict:
    debug("Calculating checksums...")
    sums = defaultdict(list)
    processed_files = 0
    with PoolExecutor(max_workers=os.cpu_count()) as executor:
        for filename, checksum in executor.map(calculate_md5, files):
            processed_files += 1
            debug(f"Processed {processed_files} out of {len(files)}")

            if len(sums[checksum]) > 0:
                info(f"Duplicate found: {sums[checksum][0]} : {filename}")

            sums[checksum].append(filename)

    debug("Finished calculating checksums!")
    return sums


def find_duplicates(sums) -> List[str]:
    duplicates = list()

    for key in sums:
        if len(sums[key]) > 1:
            duplicates += sums[key][1:]

    info(f"No of duplicates found: {len(duplicates)}")
    return duplicates


def process_files(dirr) -> List[str]:
    all_files = list_files_in_directory(dirr)
    debug(f"No of files found: {len(all_files)}")
    sums = calculate_checksums(all_files)
    return find_duplicates(sums)


def write_out_duplicate_files(duplicates, out) -> None:
    with open(out, "w") as f:
        for file in duplicates:
            f.write(file + "\n")


def remove_found_duplicates(duplicates) -> None:
    for f in duplicates:
        os.remove(f)
        info(f"Deleted duplicate file {f}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", help="Directory to work on", type=str)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-o", "--out", help="Write duplicate names to the file", type=str)
    parser.add_argument("-X", "--delete", help="Delete found duplicates", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    duplicates = process_files(args.dir)

    if args.out is not None:
        write_out_duplicate_files(duplicates, args.out)

    if args.delete is True:
        remove_found_duplicates(duplicates)


if __name__ == "__main__":
    main()
