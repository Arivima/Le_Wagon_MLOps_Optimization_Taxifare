import logging

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get the root logger to make it accessible across modules
logger = logging.getLogger(__name__)