"""
Symbol mapping for Yahoo Finance ticker symbols.
Some tickers need to be suffixed with .DE for German stocks or have special formats.
"""

SYMBOL_MAPPING = {
    # German DAX stocks
    "ADS": "ADS.DE",
    "BAS": "BAS.DE",
    "BAYN": "BAYN.DE",
    "BEI": "BEI.DE",
    "BMW": "BMW.DE",
    "BRK.B": "BRK-B",
    "CBK": "CBK.DE",    "DB1": "DB1.DE",
    "DBK": "DBK.DE",
    "DHL": "0N08.IL",  # Deutsche Post/DHL uses 0N08.IL on Yahoo Finance
    "EOAN": "EOAN.DE",
    "FME": "FME.DE",
    "FRE": "FRE.DE",
    "HEN": "HEN3.DE",  # Henkel uses HEN3.DE on Yahoo Finance
    "HNR1": "HNR1.DE",
    "IFX": "IFX.DE",
    "MUV2": "MUV2.DE",
    "P911": "P911.DE",
    "PAH3": "PAH3.DE",
    "QIA": "QIA.DE",
    "RHM": "RHM.DE",
    "RWE": "RWE.DE",
    "SHL": "SHL.DE",
    "SIE": "SIE.DE",
    "SRT3": "SRT3.DE",
    "SY1": "SY1.DE",
    "VNA": "VNA.DE",
    "VOW3": "VOW3.DE",
    "ZAL": "ZAL.DE",
}

def get_yahoo_symbol(symbol):
    """
    Get the Yahoo Finance ticker symbol for a given symbol.
    
    Args:
        symbol (str): The original symbol
        
    Returns:
        str: The Yahoo Finance ticker symbol
    """
    return SYMBOL_MAPPING.get(symbol, symbol)
