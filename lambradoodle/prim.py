
import boto3
import base64
import zlib
import json
import os
import io
import zipfile

FUNCTION_NAME = "kevin"
runtime = "python3.7"
memory=1500
timeout=60*3
LAMBDA_ROLE='arn:aws:iam::145258838769:role/EigensheepLambdaRole'

LAMBDA_TEMPLATE_PYTHON = open(
    os.path.join(os.path.dirname(__file__), "handler.py"), "r"
).read()

import base64
import os

import glob2


def create_mod_data(mod_paths):
    module_data = {}
    # load mod paths
    for m in mod_paths:
        if os.path.isdir(m):
            files = glob2.glob(os.path.join(m, "**/*.py"))
            pkg_root = os.path.abspath(os.path.dirname(m))
        else:
            pkg_root = os.path.abspath(os.path.dirname(m))
            files = [m]
        for f in files:
            f = os.path.abspath(f)
            mod_str = open(f, 'rb').read()

            dest_filename = f[len(pkg_root)+1:].replace(os.sep, "/")
            module_data[dest_filename] = mod_str

    return module_data

def zipdir(ziph, path, realpath):
    for root, dirs, files in os.walk(realpath):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                os.path.normpath(
                    os.path.join(path, os.path.relpath(root, realpath), file)
                ),
            )

def zipstr(ziph, path, contents):

    info = zipfile.ZipInfo(path)
    info.external_attr = 0o555 << 16
    ziph.writestr(info, contents)

def build_minimal_lambda_package():
    pseudofile = io.BytesIO()
    zipf = zipfile.ZipFile(pseudofile, "w", zipfile.ZIP_DEFLATED)
    zipstr(zipf, "main.py", LAMBDA_TEMPLATE_PYTHON)
    zipf.close()
    return pseudofile.getvalue()


def dencode(data):
    import dill, zlib, base64
    return base64.b64encode(zlib.compress(dill.dumps(data, 2))).decode("utf-8")

def pencode(data):
    import pickle, zlib, base64
    return base64.b64encode(zlib.compress(pickle.dumps(data, 2))).decode("utf-8")

class LambdaCaller():
    def __init__(self, lambdaClient, fn, ver):
        self.lambdaClient = lambdaClient
        self.fn = fn
        self.ver = ver

    def __call__(self, val):
        import dill
        r = self.lambdaClient.invoke(
            FunctionName=self.fn,
            InvocationType="RequestResponse",
            Payload=json.dumps({'data':pencode(val)}).encode("utf-8"),
            Qualifier=self.ver
        )
        read = r['Payload'].read()
        em = '{"errorMessage": '
        if len(read) > len(em) and read[:len(em)].decode("utf-8") == em:
            print(read)
            js = json.loads(read)
            for a in js['stackTrace']:
                print(a)
        # print(read, len(read) > len(em), read[:len(em)].decode("utf-8"), em, read[:len(em)].decode("utf-8") == em)
        return dill.loads(zlib.decompress(base64.b64decode(read)))

class LambdaExecutor():
    def __init__(self):   
        session = boto3.session.Session()
        self.lambdaClient = session.client("lambda")
        self.s3 = session.client("s3")
        self.layerCache = {}
        
        from json import dumps
        import dill

        zipfile = build_minimal_lambda_package()
        try:
            
            self.lambdaClient.delete_function(FunctionName=FUNCTION_NAME)
        except:
            pass
        try:
            self.lambdaClient.create_function(
                        FunctionName=FUNCTION_NAME, 
                        Runtime=runtime,
                        MemorySize=memory,
                        Timeout=timeout,
                        Code={
                            'ZipFile': zipfile
                        },
                        Handler='main.install_handler',
                        Publish=True,
                        Role=LAMBDA_ROLE,
                        Description='Lambdu Parallel Lambda Worker',
                        #VpcConfig={'SubnetIds': ['subnet-3e0f2c67', 'subnet-73a5cd16', 'subnet-7e316055', 'subnet-37e5ec40', 'subnet-52aa506f', 'subnet-c56affc9'], 'SecurityGroupIds': ['sg-b87841df']},
                        #FileSystemConfigs=[{"Arn":"arn:aws:elasticfilesystem:us-east-1:145258838769:access-point/fsap-06c09c9130a303e84", "LocalMountPath":"/mnt/efs"}]
                    )
        except Exception as e:
            print(e)

    def makeMapper(self, fn, packages=[], modules=[]):
        packages = list(packages)
        if 'dill' not in packages:
            packages.append('dill')
        from json import dumps
        import dill

        ib = self.lambdaClient.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({'function':dencode(fn), 'packages':pencode(packages), 'modules':pencode(create_mod_data(modules))}).encode("utf-8"),
        )
        pay = ib['Payload'].read().decode("utf-8")
        if '\n' not in pay:
            print(pay)
            js = json.loads(pay)
            for a in js['stackTrace']:
                print(a)
        fn, ver = pay.split('\n')
        return LambdaCaller(self.lambdaClient, fn, ver)

    def makeMapper2(self, fn, packages=[], modules=[]):
        packages = list(packages)
        if 'dill' not in packages:
            packages.append('dill')
        modules = create_mod_data(modules)
        import tempfile, os, shutil

        import io
        import zipfile, dill

        pseudofile = io.BytesIO()
        zipf = zipfile.ZipFile(pseudofile, "w", zipfile.ZIP_DEFLATED)

        zipstr(zipf, "main.py", LAMBDA_TEMPLATE_PYTHON)
        zipstr(zipf, "fndata", zlib.compress(dill.dumps(fn, 2)))


        mpath = "python_lambda_mods"

        import boto3

                    
        for m_filename, m_data in modules.items():
            m_path = os.path.dirname(m_filename)

            if len(m_path) > 0 and m_path[0] == "/":
                m_path = m_path[1:]
            to_make = os.path.join(mpath, m_path)
            full_filename = os.path.join(to_make, os.path.basename(m_filename))
            zipstr(zipf, full_filename, m_data)

        #from pyfakefs.fake_filesystem_unittest import Patcher
        #from pyfakefs.fake_filesystem import FakeFilesystem

        layerhash = str(hash(tuple(sorted(packages))))
        if layerhash not in self.layerCache:
            pseudofile2 = io.BytesIO()
            zipf2 = zipfile.ZipFile(pseudofile2, "w", zipfile.ZIP_DEFLATED)

            #with Patcher() as patcher:
            with tempfile.TemporaryDirectory() as path:
                #path = "/tmp/lambs"
                #shutil.rmtree(path, ignore_errors=True)
                #os.mkdir(path)
                with open(path+'/requirements.txt', 'w') as out:
                    for package in packages:
                        out.write(package+'\n')
                uid = str(os.getuid())+':'+str(os.getgid())
                os.system("docker run -v "+path+":/var/task lambci/lambda:build-python3.7 /bin/sh -c \"pip install --no-cache-dir --disable-pip-version-check -r requirements.txt -q -t pkgs; chown "+uid+" -R pkgs \"")
                zipdir(zipf2, "python_lambda_deps", path+"/pkgs")

            zipf2.close()
            zipfile2 = pseudofile2.getvalue()
            LAYER_NAME='lname'
            BUCKET='eigensheep-145258838769'
            self.s3.upload_fileobj(io.BytesIO(zipfile2), BUCKET, layerhash)
            lay = self.lambdaClient.publish_layer_version(
                        LayerName=LAYER_NAME,
                        Content={'S3Bucket':BUCKET,'S3Key':layerhash}
            )["LayerVersionArn"]
            self.layerCache[layerhash] = lay


        zipf.close()
        zipfile = pseudofile.getvalue()
        FUNCTION_NAME = "kevin2_" + layerhash
        try:
            res = self.lambdaClient.create_function(
                        FunctionName=FUNCTION_NAME, 
                        Runtime=runtime,
                        MemorySize=memory,
                        Timeout=timeout,
                        Code={
                            'ZipFile': zipfile
                        },
                        Handler='main.lambda_handler',
                        Publish=True,
                        Role=LAMBDA_ROLE,
                        Layers=[self.layerCache[layerhash]],
                        Description='Lambdu Parallel Lambda Worker'
                    )
        except:
            res = self.lambdaClient.update_function_code(
                        FunctionName=FUNCTION_NAME, 
                        ZipFile=zipfile,
                        Publish=True
                    )
        return LambdaCaller(self.lambdaClient, FUNCTION_NAME, res['Version'])

if __name__ == '__main__':
    from concurrent.futures import ThreadPoolExecutor
    import os
    print(os.popen)
    import numpy as np

    lx = LambdaExecutor()
    #print(lx.makeMapper(lambda x: os.popen('ls /mnt/efs').read())(0))

    remfn = lx.makeMapper2(np.sum, ['numpy'])
    maxfn = lx.makeMapper2(np.max, ['numpy'])

    data = [np.array([1*x, 2*x, 3*x, 4*x]) for x in range(3)]
    ThreadPoolExecutor(max_workers=len(data))
    executor = ThreadPoolExecutor(max_workers=len(data))
    print(list(executor.map(remfn, data)))
    print(list(executor.map(maxfn, data)))