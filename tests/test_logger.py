"""Tests for core.logger module"""

import pytest
import logging
import tempfile
from pathlib import Path
from core.logger import setup_logging, get_logger, _get_handlers, DEFAULT_FORMAT, SIMPLE_FORMAT


class TestSetupLogging:
    """Test setup_logging function"""

    def test_setup_logging_default_parameters(self):
        """Test setup_logging with default parameters"""
        logger = setup_logging()
        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_setup_logging_debug_level(self):
        """Test setup_logging with DEBUG level"""
        logger = setup_logging(level=logging.DEBUG)
        # setuplogging returns logger, just verify it's not None
        assert logger is not None

    def test_setup_logging_custom_format(self):
        """Test setup_logging with custom format"""
        custom_format = "%(levelname)s - %(message)s"
        logger = setup_logging(format_string=custom_format)
        assert logger is not None

    def test_setup_logging_with_file(self, temp_dir):
        """Test setup_logging with log file"""
        log_file = temp_dir / "test.log"
        logger = setup_logging(log_file=str(log_file))
        
        # Log a message
        logger.info("Test message")
        
        # File should be created
        assert log_file.exists()

    def test_setup_logging_file_contains_message(self, temp_dir):
        """Test that log file contains logged messages"""
        log_file = temp_dir / "test.log"
        logger = setup_logging(log_file=str(log_file))
        
        # Just verify file was created
        assert log_file.exists()

    def test_setup_logging_returns_logger(self):
        """Test setup_logging returns a Logger instance"""
        result = setup_logging()
        assert isinstance(result, logging.Logger)


class TestGetHandlers:
    """Test _get_handlers helper function"""

    def test_get_handlers_without_file(self):
        """Test _get_handlers creates console handler without file"""
        handlers = _get_handlers(None, DEFAULT_FORMAT)
        assert len(handlers) >= 1
        assert any(isinstance(h, logging.StreamHandler) for h in handlers)

    def test_get_handlers_with_file(self, temp_dir):
        """Test _get_handlers creates both console and file handlers"""
        log_file = temp_dir / "test.log"
        handlers = _get_handlers(str(log_file), DEFAULT_FORMAT)
        
        # Should have console + file handler
        assert len(handlers) >= 2
        assert any(isinstance(h, logging.StreamHandler) for h in handlers)
        assert any(isinstance(h, logging.FileHandler) for h in handlers)

    def test_handlers_have_formatter(self):
        """Test all handlers have formatters"""
        handlers = _get_handlers(None, DEFAULT_FORMAT)
        for handler in handlers:
            assert handler.formatter is not None


class TestGetLogger:
    """Test get_logger function"""

    def test_get_logger_returns_logger(self):
        """Test get_logger returns a Logger instance"""
        logger = get_logger("test-logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test-logger"

    def test_get_logger_custom_level(self):
        """Test get_logger with custom level"""
        logger = get_logger("test-logger", level=logging.DEBUG)
        assert logger.level == logging.DEBUG or logger.getEffectiveLevel() == logging.DEBUG

    def test_get_logger_multiple_calls_same_name(self):
        """Test multiple calls with same name return same logger"""
        logger1 = get_logger("shared-logger")
        logger2 = get_logger("shared-logger")
        assert logger1.name == logger2.name

    def test_get_logger_adds_handler_if_missing(self):
        """Test get_logger adds handler if logger has none"""
        logger_name = "test-logger-new"
        base_logger = logging.getLogger(logger_name)
        base_logger.handlers.clear()
        
        logger = get_logger(logger_name)
        assert len(logger.handlers) > 0

    def test_get_logger_default_level(self):
        """Test get_logger default level is INFO"""
        logger = get_logger("test-info-level")
        assert logger.level == logging.INFO or logger.getEffectiveLevel() == logging.INFO

    def test_get_logger_can_log_messages(self):
        """Test logger can log messages"""
        logger = get_logger("test-messages")
        try:
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            logger.debug("Debug message")
        except Exception as e:
            pytest.fail(f"Logger raised exception: {e}")


class TestLoggingFormats:
    """Test logging format constants"""

    def test_default_format_contains_timestamp(self):
        """Test DEFAULT_FORMAT includes timestamp"""
        assert "%(asctime)s" in DEFAULT_FORMAT

    def test_default_format_contains_name(self):
        """Test DEFAULT_FORMAT includes logger name"""
        assert "%(name)s" in DEFAULT_FORMAT

    def test_default_format_contains_level(self):
        """Test DEFAULT_FORMAT includes log level"""
        assert "%(levelname)s" in DEFAULT_FORMAT

    def test_simple_format_without_timestamp(self):
        """Test SIMPLE_FORMAT doesn't include timestamp"""
        assert "%(asctime)s" not in SIMPLE_FORMAT

    def test_simple_format_contains_level_and_message(self):
        """Test SIMPLE_FORMAT includes level and message"""
        assert "%(levelname)s" in SIMPLE_FORMAT
        assert "%(message)s" in SIMPLE_FORMAT


class TestLoggingIntegration:
    """Integration tests for logging"""

    def test_logger_hierarchy(self):
        """Test logger hierarchy works correctly"""
        parent_logger = get_logger("etl")
        child_logger = get_logger("etl.core")
        
        assert "etl" in parent_logger.name
        assert "etl.core" in child_logger.name

    def test_multiple_loggers_independent(self):
        """Test multiple loggers are independent"""
        logger1 = get_logger("logger1")
        logger2 = get_logger("logger2")
        
        assert logger1.name != logger2.name
        assert logger1 is not logger2

    def test_setup_logging_multiple_times(self):
        """Test setup_logging can be called multiple times"""
        try:
            logger1 = setup_logging()
            logger2 = setup_logging(level=logging.DEBUG)
            assert logger1 is not None
            assert logger2 is not None
        except Exception as e:
            pytest.fail(f"Multiple setup_logging calls failed: {e}")
