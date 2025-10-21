"""
Environment Configuration Validator

This module provides utilities for validating required environment variables
and logging helpful error messages when they are missing.
"""

import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when required configuration is missing"""
    pass


class ConfigValidator:
    """Validates environment configuration and provides helpful error messages"""
    
    @staticmethod
    def get_required_env(
        key: str,
        description: str,
        example: Optional[str] = None
    ) -> str:
        """
        Get a required environment variable with helpful error message
        
        Args:
            key: Environment variable name
            description: Description of what the variable is for
            example: Example value for the variable
            
        Returns:
            The environment variable value
            
        Raises:
            ConfigurationError: If the variable is not set
        """
        value = os.getenv(key)
        
        if not value:
            error_msg = f"""
❌ CONFIGURATION ERROR: Missing required environment variable

Variable: {key}
Description: {description}
{"Example: " + example if example else ""}

Please add this variable to your .env file:
    {key}={"<your_value>" if not example else example}

See .env.example for more details.
"""
            logger.error(error_msg)
            raise ConfigurationError(f"Missing required environment variable: {key}")
        
        return value
    
    @staticmethod
    def get_env_with_default(
        key: str,
        default: Any,
        description: str,
        env_type: type = str
    ) -> Any:
        """
        Get environment variable with default value and type conversion
        
        Args:
            key: Environment variable name
            default: Default value if not set
            description: Description of the variable
            env_type: Type to convert the value to (str, int, float, bool)
            
        Returns:
            The environment variable value or default
        """
        value = os.getenv(key)
        
        if not value:
            logger.debug(f"Using default value for {key}: {default} ({description})")
            return default
        
        try:
            if env_type == bool:
                return value.lower() in ('true', '1', 'yes', 'on')
            elif env_type == int:
                return int(value)
            elif env_type == float:
                return float(value)
            else:
                return value
        except (ValueError, AttributeError) as e:
            logger.warning(
                f"Invalid value for {key}: {value}. "
                f"Expected {env_type.__name__}. Using default: {default}"
            )
            return default
    
    @staticmethod
    def validate_required_vars(required_vars: Dict[str, str]) -> None:
        """
        Validate multiple required environment variables
        
        Args:
            required_vars: Dict of {var_name: description}
            
        Raises:
            ConfigurationError: If any required variable is missing
        """
        missing_vars = []
        
        for var_name, description in required_vars.items():
            if not os.getenv(var_name):
                missing_vars.append((var_name, description))
        
        if missing_vars:
            error_msg = "\n❌ CONFIGURATION ERROR: Missing required environment variables\n\n"
            
            for var_name, description in missing_vars:
                error_msg += f"Variable: {var_name}\n"
                error_msg += f"Description: {description}\n\n"
            
            error_msg += "Please add these variables to your .env file.\n"
            error_msg += "See .env.example for reference.\n"
            
            logger.error(error_msg)
            raise ConfigurationError(f"Missing {len(missing_vars)} required environment variable(s)")
    
    @staticmethod
    def log_configuration(config_dict: Dict[str, Any], mask_keys: Optional[List[str]] = None) -> None:
        """
        Log current configuration (masking sensitive values)
        
        Args:
            config_dict: Dictionary of configuration values
            mask_keys: List of keys to mask (e.g., API keys, passwords)
        """
        mask_keys = mask_keys or ['key', 'password', 'secret', 'token']
        
        logger.info("="*80)
        logger.info("CURRENT CONFIGURATION")
        logger.info("="*80)
        
        for key, value in sorted(config_dict.items()):
            # Mask sensitive values
            should_mask = any(mask_word in key.lower() for mask_word in mask_keys)
            
            if should_mask and value:
                display_value = f"{value[:4]}...{value[-4:]}" if len(str(value)) > 8 else "***"
            else:
                display_value = value
            
            logger.info(f"{key}: {display_value}")
        
        logger.info("="*80)


# Convenience functions
def require_env(key: str, description: str, example: Optional[str] = None) -> str:
    """Shorthand for ConfigValidator.get_required_env"""
    return ConfigValidator.get_required_env(key, description, example)


def get_env(key: str, default: Any, description: str = "", env_type: type = str) -> Any:
    """Shorthand for ConfigValidator.get_env_with_default"""
    return ConfigValidator.get_env_with_default(key, default, description, env_type)
