# Standard notification configurations
TELEGRAM_TEMPLATES = {
    'signal': """
{emoji} *{symbol}* - {signal_type} Signal

💰 *Price:* {close_price:.2f}
📊 *Confidence:* {confidence_pct}%
⏰ *Time:* {timestamp}

📝 *Analysis:*
{reason}

📈 *Technical Indicators:*
RSI: {rsi:.1f}
MACD: {macd:.2f}
Signal Line: {signal_line:.2f}
ADX: {adx:.1f}

#Signal #{symbol} #{signal_type_lower}
""",    'error': """
⚠️ *System Alert*

*Error:* {error_type}
*Details:* {error_message}
*Time:* {timestamp}

#Alert #Error
""",
    'summary': """
📊 *Daily Trading Summary*
Date: {date}

Signals Generated: {signal_count}
- BUY: {buy_count}
- SELL: {sell_count}
- NEUTRAL: {neutral_count}

Average Confidence: {avg_confidence:.1f}%
Success Rate: {success_rate:.1f}%

#Summary #Daily
"""
}
