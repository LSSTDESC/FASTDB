import argparse

from db import DB
from parquet_export import dump_to_parquet


def parse_args(argv):
    parser = argparse.ArgumentParser("Dump database to a parquet file")
    parser.add_argument("filepath", help="Path to output parquet file")
    parser.add_argument("procver", type=int, help="Processing version")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    with DB() as conn, open(args.filepath, "wb") as fp:
        dump_to_parquet(fp, procver=args.procver, connection=conn)


if __name__ == '__main__':
    main()
