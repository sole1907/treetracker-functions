# import function hello from current directory refresh_view.py
import sys
import refresh_view
import unittest

print(sys.path)

class TestStringMethods(unittest.TestCase):
    def test_hello(self):
        self.assertEqual(refresh_view.hello(), 'Hello, world!')

# run test
if __name__ == '__main__':
    unittest.main()


