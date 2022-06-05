import unittest
from app import app


class TestWorstMovieAPI(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    def test_response(self):
        response = self.app.get('/worstmovie/gap/find')

        # Check the response code
        self.assertEqual(200, response.status_code)

        # Check if all keys are in response
        self.assertIn('min', response.json)
        self.assertIn('max', response.json)
        self.assertEqual(list, type(response.json['max']))
        self.assertEqual(list, type(response.json['min']))

