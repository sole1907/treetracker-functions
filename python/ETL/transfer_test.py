import unittest
import transfer

class Test_Transfer(unittest.TestCase):
    def test(self):
        import psycopg2
        # read env variables DB_URL
        import os
        print("transfer data")
        # read env variables DB_URL
        DB_URL = os.environ['DB_URL']
        # need SRC DB_URL and TARGET DB_URL
        # need organization id
        print("DB_URL:", DB_URL)
        src_conn = psycopg2.connect(DB_URL, sslmode='require')
        dest_conn = psycopg2.connect(DB_URL, sslmode='require')
        transfer(dest_conn,src_conn, 11, action = False)

        return 
    
if __name__ == '__main__':
   
    unittest.main()