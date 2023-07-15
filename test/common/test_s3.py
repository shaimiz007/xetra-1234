"""TsetS3BucketConnectorMethonds"""
import os
import unittest

import boto3
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3bucketConnector class
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mock s3 bucket
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = "AWS_ACESS_KEY"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        # Creating s3 acess key and secret key
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "kEY2"
        # Creating s3 bucket
        self.s3 = boto3.resource('s3',endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})


        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                               self.s3_secret_key,
                                               self.s3_endpoint_url,
                                               self.s3_bucket_name)


    def tearDown(self) -> None:
        """
        excuting after unittests
        """
        # mocking S3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 file key
        as list on the mock S3 bucket
        """

        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content,Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content,Key=key2_exp)
       
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
      
        # Test after method execution
        self.assertEqual(len(list),2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        # Cleanup
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )
           
    def test_list_files_in_prefix_wrong_prefix(self):

        """
        Tests the list_files_in_prefix method for getting 0 or wrong file key
        as list on the mock S3 bucket
        """
        # Expected results
        pass
     
if __name__ == '__main__':
    unittest.main()