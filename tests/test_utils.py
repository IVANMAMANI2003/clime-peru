"""
Tests para utilidades del sistema.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from batch.utils import (
    validate_sensor_reading,
    calculate_anomaly,
    AnomalyResult
)


class TestValidateSensorReading:
    """Tests para validación de lecturas del sensor."""
    
    def test_valid_temperature(self):
        """Test: temperatura válida retorna True."""
        assert validate_sensor_reading(15.0) is True
        assert validate_sensor_reading(-20.0) is True
        assert validate_sensor_reading(40.0) is True
    
    def test_invalid_temperature_too_low(self):
        """Test: temperatura muy baja retorna False."""
        assert validate_sensor_reading(-25.0) is False
    
    def test_invalid_temperature_too_high(self):
        """Test: temperatura muy alta retorna False."""
        assert validate_sensor_reading(45.0) is False
    
    def test_custom_range(self):
        """Test: rango personalizado."""
        assert validate_sensor_reading(50.0, 0, 100) is True
        assert validate_sensor_reading(-10.0, 0, 100) is False


class TestCalculateAnomaly:
    """Tests para cálculo de anomalías."""
    
    def test_temperature_within_range(self):
        """Test: temperatura dentro del rango histórico."""
        result = calculate_anomaly(
            temperatura_actual=15.0,
            promedio_historico=12.0,
            desviacion_estandar=2.5,
            sigma_threshold=2.0
        )
        
        assert result.es_anomalia is False
        assert "dentro del rango histórico" in result.mensaje
    
    def test_temperature_above_range(self):
        """Test: temperatura por encima del rango."""
        result = calculate_anomaly(
            temperatura_actual=20.0,
            promedio_historico=12.0,
            desviacion_estandar=2.5,
            sigma_threshold=2.0
        )
        
        assert result.es_anomalia is True
        assert result.diferencia_promedio > 0
        assert "superior" in result.mensaje
    
    def test_temperature_below_range(self):
        """Test: temperatura por debajo del rango."""
        result = calculate_anomaly(
            temperatura_actual=5.0,
            promedio_historico=12.0,
            desviacion_estandar=2.5,
            sigma_threshold=2.0
        )
        
        assert result.es_anomalia is True
        assert result.diferencia_promedio < 0
        assert "inferior" in result.mensaje
    
    def test_boundaries(self):
        """Test: temperaturas en los límites exactos."""
        result = calculate_anomaly(
            temperatura_actual=17.0,  # 12 + 2*2.5 = 17
            promedio_historico=12.0,
            desviacion_estandar=2.5,
            sigma_threshold=2.0
        )
        
        assert result.es_anomalia is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])