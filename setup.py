from distutils.core import setup, Extension

setup(
    ext_modules=[
        Extension(
            "pygear",
            ["pygear.c"],
            extra_link_args=["-lgearman"],
            extra_compile_args=["-I/usr/include"]
        ),
    ],
)
