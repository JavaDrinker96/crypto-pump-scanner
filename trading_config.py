"""
Trading Configuration Module
Читает TP/SL проценты из .env и вычисляет цены
Выставляет SL и TP при открытии позиции через Bybit API
"""

import os
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class TradingConfig:
    """Конфигурация торговли из .env переменных"""
    
    # ===== LONG (PUMP) =====
    PUMP_SL_PCT = float(os.getenv('PUMP_SL_PCT', '0.015'))
    PUMP_TP1_PCT = float(os.getenv('PUMP_TP1_PCT', '0.03'))
    PUMP_TP2_PCT = float(os.getenv('PUMP_TP2_PCT', '0.05'))
    PUMP_TP3_PCT = float(os.getenv('PUMP_TP3_PCT', '0.08'))
    
    PUMP_TP1_CLOSE_PCT = float(os.getenv('PUMP_TP1_CLOSE_PCT', '0.50'))
    PUMP_TP2_CLOSE_PCT = float(os.getenv('PUMP_TP2_CLOSE_PCT', '0.30'))
    PUMP_TP3_CLOSE_PCT = float(os.getenv('PUMP_TP3_CLOSE_PCT', '0.20'))
    
    PUMP_TRAILING_PCT = float(os.getenv('PUMP_TRAILING_PCT', '0.02'))
    
    # ===== SHORT (DUMP) =====
    SHORT_SL_PCT = float(os.getenv('SHORT_SL_PCT', '0.015'))
    SHORT_TP1_PCT = float(os.getenv('SHORT_TP1_PCT', '0.03'))
    SHORT_TP2_PCT = float(os.getenv('SHORT_TP2_PCT', '0.05'))
    SHORT_TP3_PCT = float(os.getenv('SHORT_TP3_PCT', '0.08'))
    
    SHORT_TP1_CLOSE_PCT = float(os.getenv('SHORT_TP1_CLOSE_PCT', '0.50'))
    SHORT_TP2_CLOSE_PCT = float(os.getenv('SHORT_TP2_CLOSE_PCT', '0.30'))
    SHORT_TP3_CLOSE_PCT = float(os.getenv('SHORT_TP3_CLOSE_PCT', '0.20'))
    
    SHORT_TRAILING_PCT = float(os.getenv('SHORT_TRAILING_PCT', '0.02'))
    
    # ===== ОБЩИЕ =====
    PUMP_LEVERAGE = float(os.getenv('PUMP_LEVERAGE', '2'))
    PUMP_RISK_BUDGET_PCT = float(os.getenv('PUMP_RISK_BUDGET_PCT', '0.20'))
    PUMP_MAX_POSITIONS = int(os.getenv('PUMP_MAX_POSITIONS', '2'))
    MIN_NOTIONAL = float(os.getenv('MIN_NOTIONAL', '2'))
    
    @classmethod
    def log_config(cls):
        """Логирует текущую конфигурацию при старте"""
        logger.info("=" * 60)
        logger.info("TRADING CONFIGURATION LOADED")
        logger.info("=" * 60)
        logger.info("LONG (PUMP) Settings:")
        logger.info(f"  SL: -{cls.PUMP_SL_PCT*100:.1f}%")
        logger.info(f"  TP1: +{cls.PUMP_TP1_PCT*100:.1f}% (close {cls.PUMP_TP1_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  TP2: +{cls.PUMP_TP2_PCT*100:.1f}% (close {cls.PUMP_TP2_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  TP3: +{cls.PUMP_TP3_PCT*100:.1f}% (close {cls.PUMP_TP3_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  Trailing: {cls.PUMP_TRAILING_PCT*100:.1f}%")
        logger.info("")
        logger.info("SHORT (DUMP) Settings:")
        logger.info(f"  SL: +{cls.SHORT_SL_PCT*100:.1f}%")
        logger.info(f"  TP1: -{cls.SHORT_TP1_PCT*100:.1f}% (close {cls.SHORT_TP1_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  TP2: -{cls.SHORT_TP2_PCT*100:.1f}% (close {cls.SHORT_TP2_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  TP3: -{cls.SHORT_TP3_PCT*100:.1f}% (close {cls.SHORT_TP3_CLOSE_PCT*100:.0f}%)")
        logger.info(f"  Trailing: {cls.SHORT_TRAILING_PCT*100:.1f}%")
        logger.info("")
        logger.info("Trading Settings:")
        logger.info(f"  Leverage: {cls.PUMP_LEVERAGE}x")
        logger.info(f"  Risk per trade: {cls.PUMP_RISK_BUDGET_PCT*100:.0f}%")
        logger.info(f"  Max positions: {cls.PUMP_MAX_POSITIONS}")
        logger.info("=" * 60)


class PriceCalculator:
    """Вычисляет SL и TP цены на основе процентов"""
    
    @staticmethod
    def calculate_long_levels(entry_price: float) -> Dict[str, float]:
        """
        Вычисляет TP и SL для LONG позиции (PUMP)
        
        Args:
            entry_price: Цена входа
            
        Returns:
            Dict с ценами SL, TP1, TP2, TP3
        """
        config = TradingConfig
        
        sl_price = entry_price * (1 - config.PUMP_SL_PCT)
        tp1_price = entry_price * (1 + config.PUMP_TP1_PCT)
        tp2_price = entry_price * (1 + config.PUMP_TP2_PCT)
        tp3_price = entry_price * (1 + config.PUMP_TP3_PCT)
        
        return {
            'entry': entry_price,
            'sl': sl_price,
            'tp1': tp1_price,
            'tp2': tp2_price,
            'tp3': tp3_price,
            'tp1_close_pct': config.PUMP_TP1_CLOSE_PCT,
            'tp2_close_pct': config.PUMP_TP2_CLOSE_PCT,
            'tp3_close_pct': config.PUMP_TP3_CLOSE_PCT,
            'trailing_pct': config.PUMP_TRAILING_PCT,
        }
    
    @staticmethod
    def calculate_short_levels(entry_price: float) -> Dict[str, float]:
        """
        Вычисляет TP и SL для SHORT позиции (DUMP)
        
        Args:
            entry_price: Цена входа
            
        Returns:
            Dict с ценами SL, TP1, TP2, TP3
        """
        config = TradingConfig
        
        # Для SHORT: SL выше входа, TP ниже входа
        sl_price = entry_price * (1 + config.SHORT_SL_PCT)
        tp1_price = entry_price * (1 - config.SHORT_TP1_PCT)
        tp2_price = entry_price * (1 - config.SHORT_TP2_PCT)
        tp3_price = entry_price * (1 - config.SHORT_TP3_PCT)
        
        return {
            'entry': entry_price,
            'sl': sl_price,
            'tp1': tp1_price,
            'tp2': tp2_price,
            'tp3': tp3_price,
            'tp1_close_pct': config.SHORT_TP1_CLOSE_PCT,
            'tp2_close_pct': config.SHORT_TP2_CLOSE_PCT,
            'tp3_close_pct': config.SHORT_TP3_CLOSE_PCT,
            'trailing_pct': config.SHORT_TRAILING_PCT,
        }


class PositionManager:
    """Управляет открытием позиций с выставлением SL/TP через API"""
    
    def __init__(self, client):
        """
        Args:
            client: CCXT Bybit клиент
        """
        self.client = client
    
    def open_long_position(self, symbol: str, entry_price: float, qty: float) -> Dict:
        """
        Открывает LONG позицию (PUMP) с SL и TP
        
        Args:
            symbol: Символ пары (например 'BEATUSDT')
            entry_price: Цена входа
            qty: Количество
            
        Returns:
            Dict с информацией о позиции и ценах
        """
        # Вычисляем цены
        levels = PriceCalculator.calculate_long_levels(entry_price)
        
        logger.info(f"🟢 Opening LONG {symbol}")
        logger.info(f"  Entry: ${levels['entry']:.8f}")
        logger.info(f"  SL: ${levels['sl']:.8f} (-{TradingConfig.PUMP_SL_PCT*100:.1f}%)")
        logger.info(f"  TP1: ${levels['tp1']:.8f} (+{TradingConfig.PUMP_TP1_PCT*100:.1f}%)")
        logger.info(f"  TP2: ${levels['tp2']:.8f} (+{TradingConfig.PUMP_TP2_PCT*100:.1f}%)")
        logger.info(f"  TP3: ${levels['tp3']:.8f} (+{TradingConfig.PUMP_TP3_PCT*100:.1f}%)")
        
        try:
            # Открываем позицию с SL и TP через API
            order = self.client.create_order(
                symbol=symbol,
                type='market',
                side='buy',
                amount=qty,
                params={
                    'stopLoss': {
                        'triggerPrice': levels['sl'],
                        'type': 'market'
                    },
                    'takeProfit': {
                        'triggerPrice': levels['tp1'],
                        'type': 'market'
                    }
                }
            )
            
            logger.info(f"✅ LONG position opened: {order['id']}")
            
            return {
                'status': 'success',
                'order': order,
                'levels': levels,
                'side': 'buy'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to open LONG position: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'levels': levels
            }
    
    def open_short_position(self, symbol: str, entry_price: float, qty: float) -> Dict:
        """
        Открывает SHORT позицию (DUMP) с SL и TP
        
        Args:
            symbol: Символ пары (например 'BEATUSDT')
            entry_price: Цена входа
            qty: Количество
            
        Returns:
            Dict с информацией о позиции и ценах
        """
        # Вычисляем цены
        levels = PriceCalculator.calculate_short_levels(entry_price)
        
        logger.info(f"🔴 Opening SHORT {symbol}")
        logger.info(f"  Entry: ${levels['entry']:.8f}")
        logger.info(f"  SL: ${levels['sl']:.8f} (+{TradingConfig.SHORT_SL_PCT*100:.1f}%)")
        logger.info(f"  TP1: ${levels['tp1']:.8f} (-{TradingConfig.SHORT_TP1_PCT*100:.1f}%)")
        logger.info(f"  TP2: ${levels['tp2']:.8f} (-{TradingConfig.SHORT_TP2_PCT*100:.1f}%)")
        logger.info(f"  TP3: ${levels['tp3']:.8f} (-{TradingConfig.SHORT_TP3_PCT*100:.1f}%)")
        
        try:
            # Открываем позицию с SL и TP через API
            order = self.client.create_order(
                symbol=symbol,
                type='market',
                side='sell',
                amount=qty,
                params={
                    'stopLoss': {
                        'triggerPrice': levels['sl'],
                        'type': 'market'
                    },
                    'takeProfit': {
                        'triggerPrice': levels['tp1'],
                        'type': 'market'
                    }
                }
            )
            
            logger.info(f"✅ SHORT position opened: {order['id']}")
            
            return {
                'status': 'success',
                'order': order,
                'levels': levels,
                'side': 'sell'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to open SHORT position: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'levels': levels
            }


# Пример использования в pump_scanner.py:
"""
from trading_config import TradingConfig, PositionManager

# При инициализации (в __init__)
TradingConfig.log_config()  # Логирует конфигурацию
position_manager = PositionManager(self.client)

# При открытии SHORT позиции
result = position_manager.open_short_position(
    symbol='BEATUSDT',
    entry_price=0.1353,
    qty=74
)

# При открытии LONG позиции
result = position_manager.open_long_position(
    symbol='MAGMA',
    entry_price=0.502,
    qty=100
)
"""
