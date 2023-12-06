import os
import json
import time
import logging
import requests
from typing import Literal


def read_cached_auth_data(cache_dir: str, username: str) -> dict | None:
    """
    Read cached auth data from cache file, if exists
    """
    auth_filepath = os.path.join(cache_dir, f"{username}_auth.json")
    cached_auth_data = None

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    # check file
    if os.path.exists(auth_filepath):
        with open(auth_filepath, "r") as f:
            cached_auth_data = json.load(f)
    return cached_auth_data


def get_new_auth_data(username: str, password: str) -> dict:
    """
    Get new auth data from tradingview, and add created_at (i.e. timestamp)
    """
    response = requests.post(
        url="https://www.tradingview.com/accounts/signin/",
        data={
            "username": username,
            "password": password,
            "remember": "on",
        },
        headers={"Referer": "https://www.tradingview.com"},
    )
    new_auth_data = response.json()
    new_auth_data["created_at"] = int(time.time())
    return new_auth_data


def cache_new_auth_data(cache_dir: str, username: str, new_auth_data: dict) -> None:
    """
    Cache new auth data to cache file, if there is no error
    """
    auth_filepath = os.path.join(cache_dir, f"{username}_auth.json")
    if "error" in new_auth_data and len(new_auth_data["error"]) == 0:
        with open(auth_filepath, "w") as f:
            json.dump(new_auth_data, f)


def get_auth_token_and_plan(
    auth_data: dict | None, state_type: Literal["new", "cache"]
) -> dict | None:
    ret = None
    if auth_data is None:
        return ret
    try:
        logging.info(f"Using {state_type} auth data")
        ret = {
            "auth_token": auth_data["user"]["auth_token"],
            "is_pro": auth_data["user"]["is_pro"],
            "pro_plan": auth_data["user"]["pro_plan"],
        }
    except Exception as e:
        logging.error(f"Getting auth from {state_type} failed: {e}")
        ret = None
    return ret


def get_auth(username: str, password: str, cache_dir: str = "cache") -> dict | None:
    """Generate auth token for tradingview

    Args:
        username (str): Username
        password (str): Password
        cache_dir (str, optional): Directory of cached files. Defaults to "cache".

    Returns:
        str: auth token
    """
    if username is None or password is None:
        return None
    cached_auth_data = read_cached_auth_data(cache_dir, username)
    new_auth_data = None

    # if not exist or not created by this function,
    # or created more than 3 days ago
    # then create new auth data
    if (
        cached_auth_data is None
        or ("created_at" not in cached_auth_data)
        or (int(time.time()) - cached_auth_data["created_at"] > 60 * 60 * 24 * 3)
    ):
        new_auth_data = get_new_auth_data(username, password)
        cache_new_auth_data(cache_dir, username, new_auth_data)

    ret = get_auth_token_and_plan(new_auth_data, "new")
    if ret is None:
        ret = get_auth_token_and_plan(cached_auth_data, "cache")

    return ret
