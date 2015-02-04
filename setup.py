from setuptools import Extension
from setuptools import setup


pygear = Extension(
    "pygear",
    sources=["pygear.c"],
    runtime_library_dirs=["/usr/lib/"],  # libgearman7
    extra_link_args=["-l:libgearman.so.7"],
    extra_compile_args=["-I/usr/local/include", "-I/usr/include/python2.6/"]
)


setup(
    name="pygear",
    version="0.9.2",
    ext_modules=[pygear],
    test_requires=[
        'pytest',
        'mock',
        'coverage',
        'flake8',
        'pylint',
        'sphinx'
    ]
)
