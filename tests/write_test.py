from packages.services.write import (
    fGetLastLineTs,
    fGetLineTs,
    fRemoveLastLine,
    fSearchStartByteOfLine,
    get_last_line_ts,
    get_symbol_pair_filepath,
    search_idx_of_bars,
)
import time
import shutil
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_base_dir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")


def create_dummy_file(filepath: str, lines: int):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        f.write("timestamp,open,high,low,close,volume\n")
        for i in range(1, lines + 1):
            f.write("{},1.0,1.0,1.0,1.0,1.0\n".format(i))


class TestClass:
    def test_write_1(self):
        assert (
            get_symbol_pair_filepath("storage", "BINANCE:BTCUSDT", "1D")
            == "storage/BINANCE:BTCUSDT/1D.csv"
        )

    def test_write_2(self):
        create_dummy_file(os.path.join(get_base_dir(), "test_write_2.csv"), 10)

    def test_write_3(self):
        filepath = os.path.join(get_base_dir(), "test_write_3.csv")
        create_dummy_file(filepath, 10)

        with open(filepath, "rb") as f:
            assert fGetLastLineTs(f) == 10

    def test_write_4(self):
        filepath = os.path.join(get_base_dir(), "test_write_4.csv")
        create_dummy_file(filepath, 10)

        with open(filepath, "rb+") as f:
            assert fGetLastLineTs(f) == 10
            fRemoveLastLine(f)
            assert fGetLastLineTs(f) == 9

    def test_write_5(self):
        filepath = os.path.join(get_base_dir(), "test_write_5.csv")
        create_dummy_file(filepath, 0)

        with open(filepath, "rb+") as f:
            assert fGetLastLineTs(f) is None

    def test_write_6(self):
        filepath = os.path.join(get_base_dir(), "test_write_6.csv")
        create_dummy_file(filepath, 1)

        with open(filepath, "rb+") as f:
            assert fGetLastLineTs(f) == 1
            fRemoveLastLine(f)
            assert fGetLastLineTs(f) is None

    def test_write_7(self):
        filepath = os.path.join(get_base_dir(), "test_write_7.csv")
        create_dummy_file(filepath, 10)

        assert get_last_line_ts(filepath) == 10

    def test_write_8(self):
        filepath = os.path.join(get_base_dir(), "test_write_8.csv")
        create_dummy_file(filepath, 0)

        assert get_last_line_ts(filepath) is None

    def test_write_9(self):
        filepath = os.path.join(get_base_dir(), "test_write_9.csv")
        create_dummy_file(filepath, 10)

        with open(filepath, "rb") as f:
            assert fGetLineTs(f, 0) == None
            assert fGetLineTs(f, len(b"timestamp,open,high,low,close,volume\n")) == 1

            f.seek(0, os.SEEK_END)
            assert fGetLineTs(f, f.tell()) == None

    def test_write_10(self):
        filepath = os.path.join(get_base_dir(), "test_write_10.csv")
        create_dummy_file(filepath, 10)

        with open(filepath, "rb") as f:
            header = f.readline()
            assert header == b"timestamp,open,high,low,close,volume\n"

            left_byte = f.tell()
            f.seek(0, os.SEEK_END)
            right_byte = f.tell()

            assert fSearchStartByteOfLine(f, left_byte, right_byte, 1) == left_byte

            b_line = fSearchStartByteOfLine(f, left_byte, right_byte, 10)

            assert fGetLineTs(f, b_line) == 10

            b_line = fSearchStartByteOfLine(f, left_byte, right_byte, 5)

            assert fGetLineTs(f, b_line) == 5

            b_line = fSearchStartByteOfLine(f, left_byte, right_byte, 1)

            assert fGetLineTs(f, b_line) == 1

            b_line = fSearchStartByteOfLine(f, left_byte, right_byte, 0.5)

            assert fGetLineTs(f, b_line) == 1

            b_line = fSearchStartByteOfLine(f, left_byte, right_byte, 11)

            assert fGetLineTs(f, b_line) == None

    def test_write_11(self):
        bars = [{"v": [float(i), 0.0, 0.0, 0.0, 0.0, 0.0]} for i in range(10)]

        assert search_idx_of_bars(bars, 0, 10, 0) == 0
        assert search_idx_of_bars(bars, 0, 10, 0.5) == 0

        assert search_idx_of_bars(bars, 0, 10, 2) == 2

        assert search_idx_of_bars(bars, 0, 10, 9) == 9

        assert search_idx_of_bars(bars, 0, 10, 8.5) == 8
        assert search_idx_of_bars(bars, 0, 10, 10) == 9

    # def test_write_10(self):
    #     bars = []
    #     t_start = int(time.time())
    #     t_end = t_start + 1000
    #     t_interval = 20
    #     for t in range(t_start, t_end, t_interval):
    #         bars.append(
    #             {
    #                 "v": [
    #                     float(t),
    #                     1.0,
    #                     1.0,
    #                     1.0,
    #                     1.0,
    #                     1.0,
    #                 ]
    #             }
    #         )

    #     tmp_storage_dir = os.path.join(
    #         os.path.dirname(__file__), "storage_test_write_2"
    #     )

    #     try:
    #         shutil.rmtree(tmp_storage_dir)
    #     except OSError:
    #         pass
    #     os.makedirs(tmp_storage_dir, exist_ok=True)

    #     writeToFile(tmp_storage_dir, "BINANCE:BTCUSDT", "1D", bars)

    #     shutil.rmtree(tmp_storage_dir)

    # def test_write_11(self):
    #     for i in range(1, 5):
    #         bars = []
    #         t_start = int(1693550020)
    #         t_end = t_start + 20 * i
    #         t_interval = 20
    #         for t in range(t_start, t_end, t_interval):
    #             bars.append(
    #                 {
    #                     "v": [
    #                         float(t),
    #                         1.0,
    #                         1.0,
    #                         1.0,
    #                         1.0,
    #                         1.0,
    #                     ]
    #                 }
    #             )

    #         tmp_storage_dir = os.path.join(
    #             os.path.dirname(__file__), "storage_test_write_3"
    #         )

    #         try:
    #             shutil.rmtree(tmp_storage_dir)
    #         except OSError:
    #             pass
    #         os.makedirs(tmp_storage_dir, exist_ok=True)

    #         writeToFile(tmp_storage_dir, "BINANCE:BTCUSDT", "1D", bars)

    #         file_path = get_symbol_pair_filepath(
    #             tmp_storage_dir, "BINANCE:BTCUSDT", "1D"
    #         )

    #         with open(
    #             file_path,
    #             "rb+",
    #         ) as f:
    #             assert fGetLastLineTs(f, remove=True) == bars[-1]["v"][0]

    #             if i > 1:
    #                 ll = fGetLastLineTs(f)
    #                 assert (
    #                     ll == bars[-2]["v"][0]
    #                 ), 'fGetLastLineTs(f) == bars[-2]["v"][0]: {} == {}, (start, end): ({}, {})'.format(
    #                     ll, bars[-2]["v"][0], t_start, t_end
    #                 )

    #         shutil.rmtree(tmp_storage_dir)
