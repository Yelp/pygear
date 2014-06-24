all: debug

clean:
	rm -rf build pygear.so
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

debug: clean
	python setup.py build_ext -g --inplace

install:
	python setup.py install

test:
	tox
