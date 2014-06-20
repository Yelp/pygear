default: debug

clean:
	rm -rf build pygear.so

debug: clean
	python setup.py build_ext -g --inplace

install:
	python setup.py install
