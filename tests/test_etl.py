"""
Tests para el módulo ETL de SENAMHI.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from batch.utils import parse_senamhi_line, extract_metadata_from_filename


class TestParseSenamhiLine:
    """Tests para el parseo de líneas SENAMHI."""
    
    def test_parse_valid_line(self):
        """Test: línea válida se parsea correctamente."""
        line = "2020 01 15 10.5 25.3 8.2"
        result = parse_senamhi_line(line)
        
        assert result is not None
        assert result['year'] == 2020
        assert result['month'] == 1
        assert result['day'] == 15
        assert result['precip'] == 10.5
        assert result['tmax'] == 25.3
        assert result['tmin'] == 8.2
    
    def test_parse_line_with_missing_data(self):
        """Test: línea con -99.9 retorna None para esos valores."""
        line = "2020 01 15 -99.9 25.3 -99.9"
        result = parse_senamhi_line(line)
        
        assert result is not None
        assert result['precip'] is None
        assert result['tmax'] == 25.3
        assert result['tmin'] is None
    
    def test_parse_invalid_line(self):
        """Test: línea inválida retorna None."""
        line = "2020 01"
        result = parse_senamhi_line(line)
        
        assert result is None
    
    def test_parse_empty_line(self):
        """Test: línea vacía retorna None."""
        result = parse_senamhi_line("")
        
        assert result is None


class TestExtractMetadata:
    """Tests para extracción de metadatos."""
    
    def test_valid_filename(self):
        """Test: filename válido extrae metadatos correctamente."""
        result = extract_metadata_from_filename(
            "POMACANCHI-CUSCO-ACOMAYO-POMACANCHI.txt"
        )
        
        assert result == ("POMACANCHI", "CUSCO", "ACOMAYO", "POMACANCHI")
    
    def test_simple_station_name(self):
        """Test: nombre de estación compuesto."""
        result = extract_metadata_from_filename(
            "YAULI-JUNIN-YAULI-MARCAPOMACOCHA.txt"
        )
        
        assert result == ("YAULI", "JUNIN", "YAULI", "MARCAPOMACOCHA")
    
    def test_invalid_filename_raises(self):
        """Test: filename inválido lanza excepción."""
        with pytest.raises(ValueError):
            extract_metadata_from_filename("invalid.txt")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])