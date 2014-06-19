from distutils.core import setup, Extension
import numpy.distutils.misc_util

setup(
    ext_pkg='pygear',
    ext_modules=[
        Extension(
            "pygear",
            ["pygear.c"],
            extra_link_args=["-lgearman"],
            extra_compile_args=["-I/usr/include"]
        ),
    ],
    include_dirs=numpy.distutils.misc_util.get_numpy_include_dirs(),

)
