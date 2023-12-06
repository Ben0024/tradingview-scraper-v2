import logging
import os
import time

from dotenv import load_dotenv


def load_config(root_dir: str):
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)-8s %(name)-6s:  %(message)s"
    )

    MODE = os.getenv("MODE") or "dev"

    # Setup loggers
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)

    # Create log file handler
    log_dir = os.path.join(root_dir, "logs", MODE)
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(
        log_dir, "scraper-{}.log".format(time.strftime("%Y%m%d"))
    )
    log_file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s"
        )
    )
    logger.addHandler(log_file_handler)

    # Read rest of the configs
    def check_load_dotenv(dotenv_path):
        dotenv_path = os.path.join(root_dir, dotenv_path)
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path)
            logger.info("loaded {}".format(dotenv_path))

    if MODE == "prod":
        check_load_dotenv(dotenv_path=".env.production.local")
        check_load_dotenv(dotenv_path=".env.production")
    else:
        check_load_dotenv(dotenv_path=".env.development.local")
        check_load_dotenv(dotenv_path=".env.development")
    check_load_dotenv(dotenv_path=".env.local")
    check_load_dotenv(dotenv_path=".env")

    TV_STORAGE_DIR = os.getenv("TV_STORAGE_DIR")
    if TV_STORAGE_DIR is None:
        logger.error("environment variable TV_STORAGE_DIR is not set")
        exit(1)

    DBNAME = os.getenv("DBNAME")
    DBUSER = os.getenv("DBUSER")
    DBPASS = os.getenv("DBPASS")
    DBHOST = os.getenv("DBHOST")
    DBPORT = os.getenv("DBPORT")
    DBURL = f"postgresql://{DBUSER}:{DBPASS}@{DBHOST}:{DBPORT}/{DBNAME}"
    os.environ["DBURL"] = DBURL

    logger.info(f"\tDBNAME: {DBNAME}")
    logger.info(f"\tDBUSER: {DBUSER}")
    logger.info(f"\tDBHOST: {DBHOST}")
    logger.info(f"\tDBPORT: {DBPORT}")
