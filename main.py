import sys
import os
from services.config import get_config
from services.snowflake_connector import SnowflakeConnector
from services.s3_handler import S3Handler
from services.logger import configure_logger
from slack_bot.core import SlackBotCore
from datetime import datetime

if __name__ == '__main__':
    # Load configuration
    config = get_config()

    # Validate critical configuration values
    required_keys = ['AWS_BUCKET_NAME', 'AWS_BUCKET_NAME_1', 'SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']
    for key in required_keys:
        if key not in config or not config[key]:
            print(f"Error: {key} is not set in the configuration.")
            sys.exit(1)

    # Ensure logs directory exists
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Initialize Snowflake Connector
    try:
        snowflake_connector = SnowflakeConnector(config)
    except Exception as e:
        print(f"Failed to initialize Snowflake connector: {e}")
        sys.exit(1)

    # Initialize S3 Handler
    try:
        s3_handler = S3Handler(config['AWS_BUCKET_NAME'])
    except Exception as e:
        print(f"Failed to initialize S3 Handler: {e}")
        sys.exit(1)

    # Initialize Logger
    log_file_name = f'logs/log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    try:
        logger = configure_logger(config['AWS_BUCKET_NAME_1'], log_file_name)
    except Exception as e:
        print(f"Failed to initialize logger: {e}")
        sys.exit(1)

    # Initialize Slack Bot Core
    try:
        slack_bot_core = SlackBotCore(config, snowflake_connector, s3_handler, logger)
    except Exception as e:
        print(f"Failed to initialize Slack Bot Core: {e}")
        sys.exit(1)

    # Start the Slack bot
    try:
        slack_bot_core.start()
    except Exception as e:
        logger.error(f"Failed to start Slack Bot: {e}")
        print(f"Failed to start Slack Bot: {e}")
        sys.exit(1)
