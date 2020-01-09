import fiber
from pathlib import Path


def line_count(fname):
    with open(fname) as f:
        return len(f.readlines())


def main():
    files = sorted(Path('.').glob('*.py'))
    pool = fiber.Pool(4)
    counts = pool.map(line_count, files)
    for f, c in zip(files, counts):
        print("{}\t{}".format(f, c))


if __name__ == '__main__':
    main()
