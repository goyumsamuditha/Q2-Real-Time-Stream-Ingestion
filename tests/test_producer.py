import unittest
from datetime import datetime

class TestProducerLogic(unittest.TestCase):
    def test_timestamp_format(self):
        """Ensure ISO timestamps format correctly for Flink's TIMESTAMP(3)"""
        raw_api_date = "2023-05-10T14:15:00.000"
        parsed_time = datetime.fromisoformat(raw_api_date)
        formatted_time = parsed_time.strftime('%Y-%m-%d %H:%M:%S.000')
        
        self.assertEqual(formatted_time, "2023-05-10 14:15:00.000")
        
    def test_datetime_casting(self):
        """"Ensure numerical values cast correctly to prevent Flink SUM() errors"""
        mock_row = {'camera_id': 'cam_99', 'turn_movement_count': '42'}
        sensor_id = str(mock_row.get('camera_id'))
        vehicle_count = int(mock_row.get('turn_movement_count', 1))
        
        self.assertEqual(type(sensor_id), str)
        self.assertEqual(type(vehicle_count), int)
        self.assertEqual(vehicle_count, 42)
        
if __name__ == "__main__":
    unittest.main()