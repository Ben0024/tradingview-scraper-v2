import json
import random
import string
from websockets import client


def generate_random_string() -> str:
    """Generate random string of length 12

    Returns:
        str: the random string
    """
    string_length = 12
    letters = string.ascii_lowercase
    random_string = "".join(random.choice(letters) for i in range(string_length))
    return random_string


def generate_quote_session_id() -> str:
    """Generate quote session id

    Returns:
        str: quote session id
    """
    return "qs_" + generate_random_string()


def generate_chart_session_id() -> str:
    """Generate chart session id

    Returns:
        str: chart session id
    """
    return "cs_" + generate_random_string()


def prepend_header(st: str) -> str:
    """Prepend header to string for websocket communication

    Args:
        st (str): the message to be sent

    Returns:
        str: the message with header
    """
    return f"~m~{len(st)}~m~{st}"


def create_message(func: str, paramList: list) -> str:
    """Create message to send to tradingview websocket from function name and parameters

    Args:
        func (str): the function name
        paramList (list): the list of parameters

    Returns:
        str: the message with header
    """
    return prepend_header(
        json.dumps({"m": func, "p": paramList}, separators=(",", ":"))
    )


async def ws_client_send_init(
    ws: client.WebSocketClientProtocol, auth_token: str, locale: list
):
    """Send auth token to websocket

    Args:
        ws (WebSocketClientProtocol): the websocket
        auth_token (str): the auth token
    """
    if auth_token is None:
        await ws.send(create_message("set_auth_token", ["unauthorized_user_token"]))
    else:
        await ws.send(create_message("set_auth_token", [auth_token]))

    await ws.send(create_message("set_locale", locale))
