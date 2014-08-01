from setuptools import setup, Extension

pygear = Extension(
        "pygear",
        sources = ["pygear.c"],
        extra_link_args=["-lgearman"],
        extra_compile_args=["-I/usr/include", "-I/usr/include/python2.7/"]
    )

setup(
    name="pygear",
    version="0.3",
    ext_modules=[pygear],
    test_requires = [
        'pytest',
        'mock',
        'coverage',
        'flake8',
        'pylint',
        'sphinx'
    ]
)
