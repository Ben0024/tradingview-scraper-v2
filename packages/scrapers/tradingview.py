import re
import time
import json
import math
import logging
import asyncio
import traceback
import multiprocessing as mp

from websockets import client
from ..services.websockets import (
    generate_chart_session_id,
    create_message,
    ws_client_send_init,
)
from ..services.chart_session_data import ChartSessionData
from ..utils.intervals import cmp_interval
from ..constants.websockets import URL, ORIGIN, HEADER, RESPONSE_TYPE


async def async_get_multiple_bars(
    logger: logging.Logger,
    auth_token: str,
    symbol_pair_list: list[tuple[str, str]],
    locale=["en", "US"],
    timeout: int = 3,
    max_bars: int = 50000,
    max_cs: int = 10,
) -> list[tuple[str, str, list, list]] | None:
    if len(symbol_pair_list) == 0:
        logger.warning("Empty symbol pair list")
        return None
    async with client.connect(
        URL, origin=ORIGIN, max_size=None, extra_headers=HEADER  # type: ignore
    ) as ws:
        await ws_client_send_init(ws, auth_token, locale)

        # create chart sessions
        max_cs = min(max_cs, len(symbol_pair_list))
        cs_id_list: list[str] = []
        cs_info: dict[str, ChartSessionData] = {}

        idx = 0
        while idx < max_cs:
            cs_id = generate_chart_session_id()
            cs_id_list.append(cs_id)
            cs_info[cs_id] = ChartSessionData(idx, cs_id)

            await ws.send(create_message("chart_create_session", [cs_id, ""]))

            # switch timezone
            await ws.send(create_message("switch_timezone", [cs_id, "Etc/UTC"]))

            # send request
            await cs_info[cs_id].send_request(ws, idx, symbol_pair_list[idx], max_bars)

            idx += 1

        complete_cnt = 0
        timeout_cnt = 0
        cont_timeout_cnt = 0
        while complete_cnt < len(symbol_pair_list):
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Timeout")
                timeout_cnt += 1
                cont_timeout_cnt += 1
                continue
            else:
                cont_timeout_cnt = 0
            finally:
                if cont_timeout_cnt >= 3:
                    logger.error("Timeout continuously 3 times")
                    break
                if timeout_cnt >= 20:
                    logger.error("Timeout 20 times")
                    break

            if re.match(r"~m~\d+~m~~h~\d+$", str(message)):
                # This is a heartbeat message
                await ws.send(message)
            else:
                segments = re.split(r"~m~\d+~m~", str(message))[1:]

                for segment in segments:
                    # check if segment is json format
                    try:
                        data = json.loads(segment)
                    except Exception as e:
                        logger.error(f"segment {segment} parse error: {e}")
                        error = True
                    else:
                        if "session_id" in data:
                            pass
                        elif "m" in data and "p" in data:
                            m = data["m"]
                            p = data["p"]

                            assert isinstance(p, list) and len(p) > 0
                            assert isinstance(p[0], str)
                            cs_id = p[0]

                            done = False
                            error = False

                            if m not in RESPONSE_TYPE:
                                logger.error(f"Error: Unknown message type: {m} {p}")
                            elif (
                                "type" in RESPONSE_TYPE[m]
                                and RESPONSE_TYPE[m]["type"] == "error"
                            ):
                                logger.error(
                                    f"Error {m}: {p}, pair: {cs_info[cs_id].current_symbol_pair}"
                                )
                                if m != "symbol_error":
                                    error = True  # ending
                            elif m == "symbol_resolved":
                                # check if interval is available for the symbol
                                data_frequency = None
                                cur_pair = cs_info[cs_id].current_symbol_pair
                                cur_interval = None if cur_pair is None else cur_pair[1]
                                if "data_frequency" in p[2]:
                                    data_frequency = p[2]["data_frequency"]

                                if (
                                    data_frequency is not None
                                    and cur_interval is not None
                                    and cmp_interval(data_frequency, cur_interval) > 0
                                ):
                                    logger.warning(
                                        f"{cur_pair} should be at least {data_frequency}"
                                    )
                            elif m == "series_completed":
                                done = True  # ending
                            elif m == "timescale_update":
                                if (
                                    "node" in p[1][f"sds_{cs_info[cs_id].chart_idx}"]
                                ):  # check if data contains bars
                                    bars = p[1][f"sds_{cs_info[cs_id].chart_idx}"]["s"]
                                    cs_info[cs_id].bars_list.append(bars)
                                    cs_info[cs_id].symbol_pair_list.append(
                                        cs_info[cs_id].current_symbol_pair
                                    )
                                    cs_info[cs_id].detail_list.append(
                                        {
                                            "status": "ok",
                                            "m": m,
                                            "p": json.dumps(p)[:100],
                                        }
                                    )
                                    cs_info[cs_id].current_symbol_pair = None

                            if done or error:
                                # end of an symbol
                                complete_cnt += 1
                                pair_num = len(symbol_pair_list)
                                one_fifth = int(math.ceil(pair_num / 20))
                                if (
                                    complete_cnt % one_fifth == 0
                                    or complete_cnt == pair_num
                                ):
                                    logger.info(f"Progress: {complete_cnt}/{pair_num}")

                                if cs_info[cs_id].current_symbol_pair is not None:
                                    cs_info[cs_id].bars_list.append(None)
                                    cs_info[cs_id].symbol_pair_list.append(
                                        cs_info[cs_id].current_symbol_pair
                                    )
                                    cs_info[cs_id].detail_list.append(
                                        {
                                            "status": "error",
                                            "m": m,
                                            "p": json.dumps(p)[:100],
                                        }
                                    )
                                    cs_info[cs_id].current_symbol_pair = None

                                if idx < len(symbol_pair_list):
                                    await cs_info[cs_id].send_request(
                                        ws,
                                        idx,
                                        symbol_pair_list[idx],
                                        max_bars,
                                    )
                                    idx += 1
                        else:
                            logger.error(f"Unknown message: {message}")

    assert len(cs_id_list) == len(
        cs_info
    ), "Logical error: len(cs_id_list) != len(cs_info)"

    bars_list = []
    for cs_id in cs_id_list:
        cs_data = cs_info[cs_id]
        bars_list += [
            (
                cs_data.symbol_pair_list[idx][0],
                cs_data.symbol_pair_list[idx][1],
                cs_data.bars_list[idx],
                cs_data.detail_list[idx],
            )
            for idx in range(len(cs_data.symbol_pair_list))
        ]
    return bars_list


def sync_get_multiple_bars(
    logger: logging.Logger,
    auth_token: str,
    symbol_pair_list: list,
    locale=["en", "US"],
    timeout: int = 3,
    max_cs: int = 10,
) -> list[tuple[str, str, list, list]] | None:
    """get bars for multiple pairs

    Args:
        logger (logging.Logger, optional): Defaults to None.
        auth_token (str, optional): Defaults to None.
        locale (list, optional): Defaults to ["en", "US"].
        symbol_pair_list (list, optional): Defaults to None.
        timeout (int, optional): Defaults to 3.
        num_processes (int, optional): Defaults to 1.

    Returns:
        list[tuple[str, str, list]]: list of tuple(symbol, interval, bars)
    """
    bars_list = []
    try:
        bars_list = asyncio.run(
            async_get_multiple_bars(
                logger=logger,
                auth_token=auth_token,
                locale=locale,
                symbol_pair_list=symbol_pair_list,
                timeout=timeout,
                max_cs=max_cs,
            )
        )
    except Exception as e:
        logger.error(f"Error getting bars for pairs: {symbol_pair_list}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error(e)
    finally:
        return bars_list


def sync_parallel_get_multiple_bars(
    logger: logging.Logger,
    auth_token: str,
    symbol_pair_list: list,
    locale=["en", "US"],
    timeout: int = 3,
    num_processes: int = 1,
    max_cs: int = 10,
) -> list[tuple[str, str, list, list]] | None:
    """get bars for multiple pairs in parallel

    Args:
        logger (logging.Logger, optional): Defaults to None.
        auth_token (str, optional): Defaults to None.
        locale (list, optional): Defaults to ["en", "US"].
        symbol_pair_list (list, optional): Defaults to None.
        timeout (int, optional): Defaults to 3.
        num_processes (int, optional): Defaults to 1.

    Returns:
        list[tuple[str, str, list]]: list of tuple(symbol, interval, bars)
    """
    result = []
    start = time.perf_counter()
    try:
        if num_processes == 1:
            result = sync_get_multiple_bars(
                logger=logger,
                auth_token=auth_token,
                locale=locale,
                symbol_pair_list=symbol_pair_list,
                timeout=timeout,
                max_cs=max_cs,
            )
        else:
            offset = int((len(symbol_pair_list) + num_processes - 1) / num_processes)
            arglist = [
                (
                    logger,
                    auth_token,
                    locale,
                    symbol_pair_list[i : i + offset],
                    timeout,
                    max_cs,
                )
                for i in range(0, len(symbol_pair_list), offset)
            ]
            with mp.Pool(processes=num_processes) as pool:
                results = pool.starmap(sync_get_multiple_bars, arglist)
                result = [item for sublist in results for item in sublist]
                pool.close()
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    else:
        end = time.perf_counter()
        duration = end - start
        logger.info(
            "Got bars for {} pairs in {:.2f} sec with {} processes".format(
                len(symbol_pair_list), duration, num_processes
            ),
            # flush=True,
        )
    finally:
        return result


# alias to sync_parallel_get_multiple_bars
get_multiple_bars = sync_parallel_get_multiple_bars
