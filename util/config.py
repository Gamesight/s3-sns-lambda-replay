import argparse
import boto3
import questionary
import json
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

class ReplayConfig(object):
    s3_bucket = None
    s3_paths = None
    lambda_functions = None
    batch_size = 2_000_000
    concurrency = 12
    bypass = False

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-b', '--bucket', help="S3 Bucket to use", default=None)
        parser.add_argument('-p', '--paths', help="Paths to replay, comma separated", default=None)
        parser.add_argument('-l', '--lambdas', help="Lambda arns to call", default=None)
        parser.add_argument('-y', '--yes', action="store_const", help="Skip the verification",
            dest="bypass", const=True, default=False)
        args = parser.parse_args()

        self.s3_bucket = self.get_s3_bucket(args)
        self.s3_paths = self.get_s3_paths(args)
        self.lambda_functions = self.get_lambda_functions(args)
        self.bypass = args.bypass
        print(self.bypass)

    def __str__(self):
        lambda_str = '\n    - '.join(self.lambda_functions)
        return f"""
#################
# Replay Config #
#################
S3 Bucket: {self.s3_bucket}
S3 Paths :
    - {self.s3_paths[0]}
    - ({len(self.s3_paths)} total)
    - {self.s3_paths[len(self.s3_paths) - 1]}

#################
Lambda Function(s):
    - {lambda_str}
Batch Size        : {self.batch_size}
Concurrency       : {self.concurrency}
Bypass            : {self.bypass}
"""

    def get_s3_bucket(self, args):
        if args.bucket:
            return args.bucket

        bucket_resp = s3.list_buckets()

        while True:
            selection = questionary.checkbox(
                "Select the bucket which contains the files to be replayed:",
                choices=[b['Name'] for b in bucket_resp['Buckets']]
            ).ask()
            if len(selection) == 0:
                print("No selection.. try again!")
            elif len(selection) == 1:
                bucket = selection[0]
                break
            else:
                print("Invalid selection.. please select one item")

        return bucket

    def get_s3_paths(self, args):
        if args.paths:
            return args.paths.split(",")

        paths = None
        prefix = ''
        while True:
            choices = [b['Prefix'] for b in self.s3_prefix_generator(self.s3_bucket, prefix)]

            # If there are no more common prefixes beneath this layer, use it as the prefix
            if len(choices) == 0:
                paths = [prefix]
                break

            selection = questionary.checkbox(
                "Browse to the path containing the files to be replayed. When at the proper path select the first and last item to be replayed:",
                choices=reversed(choices)
            ).ask()

            if len(selection) == 0:
                print("No selection.. try again!")
            elif len(selection) == 1:
                prefix = selection[0]
            elif len(selection) == 2:
                paths = [c for c in choices if c >= min(selection) and c <= max(selection)]
                break
            else:
                print("Invalid selection.. please select the first and last item in the range to be replayed")

        return paths

    def get_lambda_functions(self, args):
        if args.lambdas:
            return args.lambdas.split(',')

        lambda_list = list(self.lambda_function_generator())

        while True:
            selection = questionary.checkbox(
                "Select the bucket which contains the files to be replayed:",
                choices=sorted([f['FunctionArn'] for f in lambda_list])
            ).ask()
            if len(selection) == 0:
                print("No selection.. try again!")
            elif len(selection) > 0:
                lambdas = selection
                break

        return lambdas

    def s3_prefix_generator(self, bucket, prefix):
        opts = {
            'Bucket': bucket,
            'Prefix': prefix,
            'Delimiter': '/',
        }
        while True:
            resp = s3.list_objects_v2(**opts)
            contents = resp.get('CommonPrefixes',[])

            for obj in contents:
                yield obj

            try:
                opts['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def lambda_function_generator(self):
        opts = {}
        while True:
            resp = lambda_client.list_functions(**opts)
            contents = resp['Functions']
            for obj in contents:
                yield obj

            try:
                opts['Marker'] = resp['NextMarker']
            except KeyError:
                break
