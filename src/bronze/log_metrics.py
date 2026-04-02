
def log_metrics(logger, raw_count, valid_count, invalid_count):
    logger.info(f"Rows Read: {raw_count}")
    logger.info(f"Valid Rows: {valid_count}")
    logger.info(f"Quarantined Rows: {invalid_count}")