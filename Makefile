all:
	rm -rf build pygear.so
	python setup.py build_ext --inplace
