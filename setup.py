from setuptools import setup, Extension

pygear = Extension(
        "pygear",
        sources = ["pygear.c"],
        extra_link_args=["-l:libgearman.so.7"],
        extra_compile_args=["-I/usr/local/include", "-I/usr/include/python2.6/"]
    )

setup(
    name="pygear",
    version="0.8",
    ext_modules=[pygear],
    test_requires = [
        'pytest',
        'mock',
        'coverage',
        'flake8',
        'pylint',
        'sphinx',
        'testifycompat'
    ]
)
