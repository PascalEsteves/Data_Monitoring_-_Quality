import logging

class LoggerFactory:
    _loggers = {}

    @staticmethod
    def get_logger(name: str = 'default') -> logging.Logger:
        if name in LoggerFactory._loggers:
            return LoggerFactory._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        LoggerFactory._loggers[name] = logger
        return logger
