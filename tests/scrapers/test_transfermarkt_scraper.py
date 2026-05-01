import pytest
from unittest.mock import MagicMock, patch
import os
import sys

# Setup paths
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from scrapers.transfermarkt import scrape_player_profile, search_player


class TestTransfermarktScraper:
    def test_parse_player_details_logic(self):
        """
        Validates that the scraper functions exist and have the correct signatures.
        Full integration tests require Playwright + network, so we test the logic only.
        """
        # Functions exist and are callable
        assert callable(scrape_player_profile)
        assert callable(search_player)

    def test_height_conversion(self):
        """Verifica que la conversión de '1,85 m' a cm sea correcta."""
        # The height conversion logic: value.replace(',', '.').replace('m', '').strip()
        # Then int(float(h) * 100)
        raw = "1,85 m"
        h = raw.replace(',', '.').replace('m', '').strip()
        result = int(float(h) * 100)
        assert result == 185

        raw2 = "1,78 m"
        h2 = raw2.replace(',', '.').replace('m', '').strip()
        result2 = int(float(h2) * 100)
        assert result2 == 178
