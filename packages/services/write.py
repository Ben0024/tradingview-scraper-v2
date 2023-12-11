import io
import os
from dotenv import load_dotenv

load_dotenv()


def record_error_file(filepath: str):
    error_file = os.getenv("ERROR_FILE") or "error_file.txt"
    with open(error_file, "a") as f:
        f.write(filepath + "\n")


def get_symbol_pair_filepath(
    storage_dir: str, symbol: str, interval: str, extension: str = ".csv"
) -> str:
    return os.path.join(storage_dir, symbol, f"{interval}{extension}")


def f_get_line_ts(file: io.BufferedReader, offset: int) -> float | None:
    """Get the timestamp of the line at offset.

    Args:
        file (io.BufferedReader)
        offset (int)

    Returns:
        float
    """
    line_timestamp = None

    try:
        file.seek(offset, os.SEEK_SET)

        if file.read(1) == b"\n":
            file.seek(-2, os.SEEK_CUR)
        else:
            file.seek(-1, os.SEEK_CUR)

        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)

        line = file.readline()
        line_timestamp_str, _ = line.split(b",", 1)
        line_timestamp = float(line_timestamp_str)

    except Exception:
        # error when empty or only one line(header line)
        # or offset is at the last empty line
        pass

    return line_timestamp


def f_get_last_line_ts(file: io.BufferedReader) -> float | None:
    last_timestamp = None
    try:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)

        # read at last line (not first line)
        last_line = file.readline().decode()

        try:
            # get last timestamp
            last_timestamp = float(last_line.rstrip().split(",")[0])
        except ValueError:
            # error when empty or only one line(header line)
            last_timestamp = None

    except OSError:
        # error when empty or only one line(header line)
        pass

    return last_timestamp


def f_remove_last_line(file: io.BufferedReader):
    try:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)

        file.truncate()
    except OSError:
        # error when empty or only one line(header line)
        pass
    except Exception:
        pass


def f_search_start_byte_of_line(
    file: io.BufferedReader, left: int, right: int, target_ts: float
) -> int:
    """Search for the start byte of the line that contains timestamp larger than or equal to target_ts.

    Args:
        file (io.BufferedReader)
        left (int)
        right (int)
        target_ts (float)

    Returns:
        int: start byte of the line
    """
    l_byte = left
    r_byte = right
    while l_byte < r_byte:
        m = (l_byte + r_byte) // 2
        ts = f_get_line_ts(file, m)
        if ts is None:
            # TODO should be handle more carefully
            record_error_file(file.name)
            return r_byte
        if ts < target_ts:
            l_byte = m + 1
        else:
            r_byte = m
    return l_byte


def search_idx_of_bars(bars: list, left: int, right: int, target_ts: float) -> int:
    """Search for the index of the bar that contains timestamp less than or equal to target_ts.

    Args:
        bars (list)
        left (int)
        right (int)
        target_ts (float)

    Returns:
        int: index of the bar
    """
    assert (
        target_ts >= bars[left]["v"][0]
    ), "target_ts {} < bars[left]['v'][0] {}".format(target_ts, bars[left]["v"][0])
    l_idx = left
    r_idx = right
    while l_idx < r_idx:
        m = (l_idx + r_idx) // 2
        ts = bars[m]["v"][0]
        if ts <= target_ts:
            l_idx = m + 1
        else:
            r_idx = m
    return l_idx - 1


def get_last_line_ts(filepath: str) -> float | None:
    """Get the timestamp of the last line in the file.

    Args:
        filepath (str)

    Returns:
        float
    """
    try:
        with open(filepath, "rb") as f:
            return f_get_last_line_ts(f)
    except OSError:
        return None


def write_empty_file(storage_dir: str, symbol: str, interval: str):
    os.makedirs(os.path.join(storage_dir, symbol), exist_ok=True)
    filepath = get_symbol_pair_filepath(storage_dir, symbol, interval)
    if not os.path.exists(filepath):
        # new file
        with open(filepath, "w") as f:
            f.write("timestamp,open,high,low,close,volume\n")


def write_to_file(storage_dir: str, symbol: str, interval: str, bars: list) -> dict:
    """write bars to csv file in storage directory,
    if file does not exist, create new file, then append to file
    else if file data do not overlap with bars, direct append to existing file
    else if file has bars that overlap with bars, check if overlapped bars are same, if same, direct append to existing file, else return warning

    Args:
        storage_dir (str)
        symbol (str)
        interval (str)
        bars (list)
    """
    res = {
        "status": "ok",
        "message": "success",
        "symbol": symbol,
        "interval": interval,
        "details": [],
    }

    os.makedirs(os.path.join(storage_dir, symbol), exist_ok=True)

    # write to csv
    filepath = get_symbol_pair_filepath(storage_dir, symbol, interval)

    if not os.path.exists(filepath):
        # new file
        with open(filepath, "w") as f:
            f.write("timestamp,open,high,low,close,volume\n")

    with open(filepath, "rb+") as f:
        # get old left and right byte and last timestamp
        f.readline()
        old_left_byte = f.tell()
        f.seek(0, os.SEEK_END)
        old_right_byte = f.tell()
        old_right_ts = f_get_last_line_ts(f)

        bars_left_ts = bars[0]["v"][0]

        if old_right_ts is not None and old_right_ts >= bars_left_ts:
            # old overlapped
            old_overlapped_bars = []
            old_overlapped_left_byte = f_search_start_byte_of_line(
                f, old_left_byte, old_right_byte, bars_left_ts
            )
            f.seek(old_overlapped_left_byte, os.SEEK_SET)
            old_overlapped_lines = f.readlines(
                old_right_byte - old_overlapped_left_byte
            )
            try:
                old_overlapped_bars = [
                    [
                        float(x.strip(" '.?!"))
                        for x in old_line.decode().strip(" '.?!").split(",")
                    ]
                    for old_line in old_overlapped_lines
                ]
            except Exception:
                record_error_file(filepath)

            # new overlapped
            bars_overlapped_right_idx = search_idx_of_bars(
                bars, 0, len(bars), old_right_ts
            )
            bars_overlapped_bars = bars[: bars_overlapped_right_idx + 1]

            # check if overlapped bars are same
            diff_overlapped_bars, diff_details = diff_old_overlapped_bars(
                old_overlapped_bars, bars_overlapped_bars
            )

            if len(diff_details) > 0:
                res["status"] = "warning"
                res["message"] = "overlapped bars are different"
                res["details"] = diff_details

            if len(diff_overlapped_bars) > 0:
                diff_filepath = get_symbol_pair_filepath(
                    storage_dir, symbol, interval, extension=".diff.csv"
                )
                with open(diff_filepath, "a") as df:
                    for diff_bar in diff_overlapped_bars:
                        df.write("{}\n".format(",".join([str(v) for v in diff_bar])))

            # truncate the old overlapped bars
            f.seek(old_overlapped_left_byte, os.SEEK_SET)
            f.truncate()

        f.seek(0, os.SEEK_END)
        # append new bars
        for bar in bars:
            assert len(bar["v"]) > 1, "bar['v'] must have at least 2 values"
            f.write("{}\n".format(",".join([str(v) for v in bar["v"]])).encode("utf-8"))

    return res


def diff_old_overlapped_bars(old_bars: list, new_bars: list) -> tuple[list, list]:
    diff_bars = []
    details = []
    if len(old_bars) == len(new_bars):
        for old_bar, new_bar in zip(old_bars, new_bars):
            new_bar_convert = [float(x) for x in new_bar["v"]]
            if old_bar != new_bar_convert:
                diff_bars.append(old_bar)
        if len(diff_bars):
            details.append(
                {
                    "msg": "old_bars and new_bars are different",
                    "len(diff_bars)": len(diff_bars),
                }
            )
    else:
        details.append(
            {
                "msg": "old_bars and new_bars have different length",
                "len(old_bars)": len(old_bars),
                "len(new_bars)": len(new_bars),
                "range(old_bars)": (old_bars[0][0], old_bars[-1][0]),
                "range(new_bars)": (new_bars[0]["v"][0], new_bars[-1]["v"][0]),
            }
        )
    return diff_bars, details
