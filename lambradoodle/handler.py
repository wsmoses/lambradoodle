import os, sys, ast, pprint, base64, zlib

FUNCTION_NAME = "kevin2"
runtime = "python3.7"
memory=1500
timeout=60*3
LAMBDA_ROLE='arn:aws:iam::145258838769:role/EigensheepLambdaRole'

sys.path.append(os.path.join(os.path.dirname(__file__), 'python_lambda_deps'))
sys.path.append('/opt/python_lambda_deps')
sys.path.append(os.path.join(os.path.dirname(__file__), 'python_lambda_mods'))


def decode(data):
    import dill, zlib, base64
    return base64.b64encode(zlib.compress(dill.dumps(data, 2))).decode('utf-8')

def zipstr(ziph, path, contents, perm=0o555):
    import zipfile

    info = zipfile.ZipInfo(path)
    info.external_attr = perm << 16
    ziph.writestr(info, contents)

def zipdir(ziph, path, realpath):
    for root, dirs, files in os.walk(realpath):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                os.path.normpath(
                    os.path.join(path, os.path.relpath(root, realpath), file)
                ),
            )

def install_handler(event, context):
    from pip import _internal

    import pickle
    print(event)
    packages = pickle.loads(zlib.decompress(base64.b64decode(event['packages'])))
    
    import sys
    import builtins
    old_open = builtins.open
    old_mkdir = os.mkdir
    old_rename = os.rename

    tmpfs = {}
    mods = {}
    class File(object):
        """A basic file-like object."""

        def __init__(self, path, tmp, *args, **kwargs):
            self._path = path
            self.tmp = tmp
            self._file = None
            if tmp and path in tmpfs:
                self._file = tmpfs[path]
                self._file.seek(0)
                print(self._file)
            if self._file is None:
                self._file = io.BytesIO()

        def seek(self, *args, **kwargs):
            return self._file.seek(*args, **kwargs)

        def tell(self, *args, **kwargs):
            return self._file.tell(*args, **kwargs)

        def seekable(self, *args, **kwargs):
            return self._file.seekable(*args, **kwargs)

        def close(self, *args, **kwargs):
            pass

        def write(self, *args, **kwargs):
            return self._file.write(*args, **kwargs)

        def read(self, *args, **kwargs):
            return self._file.read(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, e_type, e_val, e_tb):
            if self.tmp:
                tmpfs[self._path] = self._file
                print("saving as ", self._path)
            else:
                zipstr(zipf, self._path, self._file.read())

    
    original_chmod = os.chmod


    def chmod_override(filename, perm):
        print("chmod", filename, perm)

        if len(filename) > 5 and filename[:5] == "/tmp/":
            mods[filename] = perm
        else:
            return original_chmod(filename, perm)


    

    def open_override(filename, *args, **kwargs):
        print(filename, args, kwargs)
        if len(filename) > 5 and filename[:5] == "/tmp/":
            return File(filename, True, *args, **kwargs)
        if len(args) == 0 or args[0] == 'r' or args[0] == 'rb' or filename == '/dev/null':
            return old_open(filename, *args, **kwargs)
        return File(filename, False, *args, **kwargs)

    def mkdir_override(filename, *args, **kwargs):
        #if len(filename) > 5 and filename[:5] == "/tmp/":
        #    return old_mkdir(filename, *args, **kwargs)
        print('mkdir', filename, args, kwargs)
        return

    def rename_override(src, dst):
        print('rename', src, dst, os.path.isdir(src))
        toplace = "/python_lambda"
        if dst[:len(toplace)] == toplace:
            if os.path.isdir(src):
                zipdir(zipf, dst[1:], src)
                import shutil
                if len(src) > 5 and src[:5] == "/tmp/":
                    pass
                else:
                    shutil.rmtree(src)
            else:
                perm = 0o555
                if src in mods:
                    perm = mods[src]
                zipstr(zipf, dst[1:], open(src, 'rb').read(), perm)
                if len(src) > 5 and src[:5] == "/tmp/":
                    pass
                else:
                    os.unlink(src)
        else:
            old_rename(src, dst)



    #builtins.open = open_override
    #os.mkdir = mkdir_override
    #os.rename = rename_override
    #os.chmod = chmod_override

    import io
    import zipfile

    pseudofile = io.BytesIO()
    zipf = zipfile.ZipFile(pseudofile, "w", zipfile.ZIP_DEFLATED)

    zipstr(zipf, "main.py", open("/var/task/main.py", "r").read())
    zipstr(zipf, "fndata", base64.b64decode(event['function']))


    import shutil

    mpath = "python_lambda_mods"

    import boto3

    modules = pickle.loads(zlib.decompress(base64.b64decode(event['modules'])))
                
    for m_filename, m_data in modules.items():
        m_path = os.path.dirname(m_filename)

        if len(m_path) > 0 and m_path[0] == "/":
            m_path = m_path[1:]
        to_make = os.path.join(mpath, m_path)
        full_filename = os.path.join(to_make, os.path.basename(m_filename))
        print("creating", full_filename)
        zipstr(zipf, full_filename, m_data)

    #from pyfakefs.fake_filesystem_unittest import Patcher
    #from pyfakefs.fake_filesystem import FakeFilesystem

    #with Patcher() as patcher:
    if len(packages):
        #patcher.fs.add_real_paths(['/var'])
        os.chdir('/tmp')
        print(os.popen('ls .').read())
        os.environ["TMP"] = "/mnt/efs"
        os.environ["TEMP"] = "/mnt/efs"
        path = "/tmp/python_lambda_deps"
        os.mkdir(path)
        shutil.rmtree(path)
        os.mkdir(path)
        print("running pip", packages, path)
        _internal.main(
            ["install", "--no-cache-dir", "--progress-bar=off", "--target=" + path]
            + packages
        )
        print("ran pip")
        zipdir(zipf, "python_lambda_deps", path)
        print("zipped")

    builtins.open = old_open

    zipf.close()
    zipfile = pseudofile.getvalue()
    session = boto3.session.Session()
    lambdaClient = session.client("lambda")
    
    try:
        res = lambdaClient.create_function(
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
                    Description='Lambdu Parallel Lambda Worker'
                )
    except:
        res = lambdaClient.update_function_code(
                    FunctionName=FUNCTION_NAME, 
                    ZipFile=zipfile,
                    Publish=True
                )
    return (FUNCTION_NAME+"\n"+res['Version']).encode("utf-8")

def lambda_handler(event, context):
    import os
    import dill, zlib, base64, os, sys
    val = dill.loads(zlib.decompress(base64.b64decode(event['data'])))
    fn = dill.loads(zlib.decompress(open('fndata', 'rb').read()))
    res = fn(val)
    return base64.b64encode(zlib.compress(dill.dumps(res, 2))).decode("utf-8")